import sys
import argparse
from asammdf import MDF
import numpy as np
import re
import os
import functools
import tempfile
import pandas as pd

# Bounded so a long-running driver process doesn't grow this without limit
# across many distinct (file, where_clause) query shapes.
_PARTITIONS_CACHE_SIZE = 32

def get_partitions(file_path, where_clause, max_records_per_partition=None):
    """Enumerate the (channel, group, index, offset, count) partitions matching
    where_clause. Spark re-invokes query planning on every action (and can
    invoke it more than once per action under AQE), so this is memoized on
    (file_path, where_clause, max_records_per_partition) plus a cheap stat()
    of the file, letting repeat calls in the same driver process skip
    re-scanning the file entirely instead of re-parsing it from scratch
    every time.

    When max_records_per_partition is set, a channel occurrence with more
    records than that is split into multiple partitions, each covering a
    disjoint [offset, offset + count) record range of the same occurrence —
    see _record_chunks. Without it (the default), each occurrence is exactly
    one partition, as before."""
    try:
        stat = os.stat(file_path)
        cache_key = (stat.st_mtime_ns, stat.st_size)
    except OSError:
        cache_key = None
    return _get_partitions_cached(file_path, where_clause, cache_key, max_records_per_partition)

@functools.lru_cache(maxsize=_PARTITIONS_CACHE_SIZE)
def _get_partitions_cached(file_path, where_clause, _cache_key, max_records_per_partition):
    mdf = MDF(file_path)
    partitions = []

    # Very simple extraction of channels from where_clause
    requested_channels = re.findall(r"'(.*?)'", where_clause)
    names = requested_channels if requested_channels else list(mdf.channels_db.keys())

    for channel_name in names:
        occurrences = mdf.channels_db.get(channel_name, ())
        for group_idx, ch_idx in occurrences:
            if not requested_channels and ch_idx == mdf.masters_db.get(group_idx):
                # Skip master/time channels when enumerating the whole file;
                # an explicit "channel = 'time'" filter is still honored above.
                continue
            # Cycle count and occurrence lookup both come straight from
            # metadata (channels_db / channel_group), so this never decodes
            # the sample arrays just to count or locate them.
            total_count = mdf.groups[group_idx].channel_group.cycles_nr
            for offset, count in _record_chunks(total_count, max_records_per_partition):
                partitions.append({
                    "channel": channel_name,
                    "group": group_idx,
                    "index": ch_idx,
                    "offset": offset,
                    "count": count,
                })

    return partitions


def _record_chunks(total_count, max_records_per_partition):
    """Split [0, total_count) into disjoint (offset, count) record ranges of
    at most max_records_per_partition records each. Falls back to a single
    (0, total_count) chunk when max_records_per_partition is unset, <= 0, or
    already covers the whole occurrence."""
    if not max_records_per_partition or max_records_per_partition <= 0 or total_count <= max_records_per_partition:
        yield (0, total_count)
        return
    offset = 0
    while offset < total_count:
        count = min(max_records_per_partition, total_count - offset)
        yield (offset, count)
        offset += count

def read_data(file_path, channel_name, where_clause="", group_index=None, channel_index=None,
              record_offset=0, record_count=None):
    """Read channel_name and return a Parquet path holding (time, channel,
    valueNumeric, valueText) rows, optionally filtered by where_clause.

    When group_index/channel_index are given, they identify the exact
    occurrence to read (as already resolved by get_partitions on the
    driver), so this reads that one occurrence directly instead of
    re-running whereis() and reading every occurrence of the name — each
    occurrence is its own partition, so re-reading all of them here would
    both redo work the driver already did and duplicate rows across
    partitions. Without them (e.g. direct/manual calls), it falls back to
    resolving and concatenating every occurrence of channel_name.

    record_offset/record_count, when given, further restrict the read to a
    [record_offset, record_offset + record_count) slice of that occurrence's
    records via asammdf's own record_offset/record_count support in
    MDF.get() — this is what lets a single large channel occurrence be split
    across multiple Spark partitions (see get_partitions /
    _record_chunks), each decoding only its own record range rather than the
    whole channel. They are only meaningful together with an explicit
    occurrence hint, matching how get_partitions produces them."""
    mdf = MDF(file_path)
    # Get the start time from header to convert relative timestamps to absolute
    start_time = mdf.header.start_time

    if group_index is not None and channel_index is not None:
        occurrences = [(group_index, channel_index)]
    else:
        # A channel name can occur in more than one group (e.g. logged on
        # multiple buses); concatenate every occurrence into a single frame.
        # All occurrences of the same channel name are assumed to share the
        # same sample type.
        occurrences = mdf.whereis(channel_name)

    frames = []
    is_numeric = True
    for group_idx, ch_idx in occurrences:
        if group_index is not None and channel_index is not None:
            get_kwargs = {"record_offset": record_offset}
            if record_count is not None:
                get_kwargs["record_count"] = record_count
            ch = mdf.get(group=group_idx, index=ch_idx, **get_kwargs)
        else:
            ch = mdf.get(group=group_idx, index=ch_idx)

        # Determine if samples are numeric or discrete
        is_numeric = np.issubdtype(ch.samples.dtype, np.number)

        # Convert relative timestamps to absolute datetime.
        # start_time is already a datetime object
        abs_timestamps = pd.to_datetime(start_time) + pd.to_timedelta(ch.timestamps, unit='s')

        # pandas chooses the datetime64 storage resolution (us vs ns) based on what the input
        # values actually need to be represented exactly — e.g. whole-second timestamps (as a
        # 1 Hz channel like battery_voltage has) resolve to `us`, while sub-second timestamps
        # resolve to `ns`. A fixed `.view(np.int64) // 1000` silently assumed `ns` and produced
        # garbage (epoch-1970-ish) timestamps for any channel that resolved to `us` instead, so
        # the resolution is forced explicitly here before reading the raw epoch integer.
        # start_time is normally tz-aware (asammdf stores the header's UTC offset), but this
        # also tolerates a tz-naive start_time.
        if abs_timestamps.tz is not None:
            abs_timestamps = abs_timestamps.tz_convert('UTC').tz_localize(None)
        epoch_micros = abs_timestamps.astype('datetime64[us]').view(np.int64)

        # Create DataFrame with both columns
        data = {
            'time': epoch_micros,  # microseconds since epoch, for Spark's TimestampType
            'channel': channel_name,
            'valueNumeric': None,
            'valueText': None
        }

        if is_numeric:
            data['valueNumeric'] = ch.samples
        else:
            # For non-numeric, convert to string and put in valueText
            data['valueText'] = ch.samples.astype(str)

        frames.append(pd.DataFrame(data))

    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
        columns=['time', 'channel', 'valueNumeric', 'valueText']
    )

    # Apply where_clause if provided. The clause already refers to the real
    # column names ("valueNumeric"/"valueText") as pushed down by Spark's filter
    # pruning, so we only need to pick out the clauses relevant to this frame.
    if where_clause and not df.empty:
        try:
            query = where_clause.replace(' = ', ' == ')

            # Split by AND and find parts that filter on the value column
            parts = [p.strip() for p in query.replace('(', '').replace(')', '').split('AND')]
            value_filters = []
            for p in parts:
                if 'valueNumeric' in p or 'valueText' in p:
                    if 'IS NOT NULL' in p or 'IS NULL' in p:
                         # pandas query handles nulls differently, but for now we skip or simplify
                         continue

                    clean_p = p
                    if "'" in p and is_numeric:
                        # If it's numeric but has quotes, remove them
                        clean_p = p.replace("'", "")

                    value_filters.append(clean_p)

            if value_filters:
                final_query = " & ".join(value_filters)
                print(f"Applying filter in Python: {final_query}", file=sys.stderr)
                df = df.query(final_query)
        except Exception as e:
            print(f"Warning: Failed to apply filter '{where_clause}' in Python: {e}", file=sys.stderr)

    # Ensure correct types for Parquet
    # valueNumeric must be float/double for Spark, even if it's currently int
    df['valueNumeric'] = df['valueNumeric'].astype(float)
    df['valueText'] = df['valueText'].astype(str)

    # Use a temporary file to pass data to Scala
    fd, path = tempfile.mkstemp(suffix='.parquet')
    try:
        os.close(fd)
        df.to_parquet(path, index=False, engine='pyarrow')
        return path
    except Exception as e:
        if os.path.exists(path):
            os.remove(path)
        raise e

def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")

    p_part = subparsers.add_parser("partitions")
    p_part.add_argument("--path", required=True)
    p_part.add_argument("--where", default="")
    p_part.add_argument("--max-records-per-partition", type=int, default=None)

    p_read = subparsers.add_parser("read")
    p_read.add_argument("--path", required=True)
    p_read.add_argument("--channel", required=True)
    p_read.add_argument("--group", type=int, default=None)
    p_read.add_argument("--index", type=int, default=None)
    p_read.add_argument("--offset", type=int, default=0)
    p_read.add_argument("--count", type=int, default=None)
    p_read.add_argument("--where", default="")

    args = parser.parse_args()

    if args.command == "partitions":
        parts = get_partitions(args.path, args.where, args.max_records_per_partition)
        for p in parts:
            print(f"{p['channel']}|{p['group']}|{p['index']}|{p['offset']}|{p['count']}")
    elif args.command == "read":
        path = read_data(args.path, args.channel, args.where, args.group, args.index,
                          args.offset, args.count)
        print(f"PARQUET:{path}")

if __name__ == "__main__":
    main()
