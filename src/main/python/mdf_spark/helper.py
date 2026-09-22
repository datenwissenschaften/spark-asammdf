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

def get_partitions(file_path, where_clause):
    """Enumerate the (channel, group, index, count) partitions matching
    where_clause. Spark re-invokes query planning on every action (and can
    invoke it more than once per action under AQE), so this is memoized on
    (file_path, where_clause) plus a cheap stat() of the file, letting
    repeat calls in the same driver process skip re-scanning the file
    entirely instead of re-parsing it from scratch every time."""
    try:
        stat = os.stat(file_path)
        cache_key = (stat.st_mtime_ns, stat.st_size)
    except OSError:
        cache_key = None
    return _get_partitions_cached(file_path, where_clause, cache_key)

@functools.lru_cache(maxsize=_PARTITIONS_CACHE_SIZE)
def _get_partitions_cached(file_path, where_clause, _cache_key):
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
            count = mdf.groups[group_idx].channel_group.cycles_nr
            partitions.append({
                "channel": channel_name,
                "group": group_idx,
                "index": ch_idx,
                "count": count,
            })

    return partitions

def read_data(file_path, channel_name, where_clause="", group_index=None, channel_index=None):
    """Read channel_name and return a Parquet path holding (time, channel,
    valueCont, valueDisc) rows, optionally filtered by where_clause.

    When group_index/channel_index are given, they identify the exact
    occurrence to read (as already resolved by get_partitions on the
    driver), so this reads that one occurrence directly instead of
    re-running whereis() and reading every occurrence of the name — each
    occurrence is its own partition, so re-reading all of them here would
    both redo work the driver already did and duplicate rows across
    partitions. Without them (e.g. direct/manual calls), it falls back to
    resolving and concatenating every occurrence of channel_name."""
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
        ch = mdf.get(group=group_idx, index=ch_idx)

        # Determine if samples are numeric or discrete
        is_numeric = np.issubdtype(ch.samples.dtype, np.number)

        # Convert relative timestamps to absolute datetime64[ns]
        # start_time is already a datetime object
        abs_timestamps = pd.to_datetime(start_time) + pd.to_timedelta(ch.timestamps, unit='s')

        # Create DataFrame with both columns
        data = {
            'time': (abs_timestamps.view(np.int64) // 1000), # Convert to microseconds for Spark
            'channel': channel_name,
            'valueCont': None,
            'valueDisc': None
        }

        if is_numeric:
            data['valueCont'] = ch.samples
        else:
            # For non-numeric, convert to string and put in valueDisc
            data['valueDisc'] = ch.samples.astype(str)

        frames.append(pd.DataFrame(data))

    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
        columns=['time', 'channel', 'valueCont', 'valueDisc']
    )

    # Apply where_clause if provided. The clause already refers to the real
    # column names ("valueCont"/"valueDisc") as pushed down by Spark's filter
    # pruning, so we only need to pick out the clauses relevant to this frame.
    if where_clause and not df.empty:
        try:
            query = where_clause.replace(' = ', ' == ')

            # Split by AND and find parts that filter on the value column
            parts = [p.strip() for p in query.replace('(', '').replace(')', '').split('AND')]
            value_filters = []
            for p in parts:
                if 'valueCont' in p or 'valueDisc' in p:
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
    # valueCont must be float/double for Spark, even if it's currently int
    df['valueCont'] = df['valueCont'].astype(float)
    df['valueDisc'] = df['valueDisc'].astype(str)

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

    p_read = subparsers.add_parser("read")
    p_read.add_argument("--path", required=True)
    p_read.add_argument("--channel", required=True)
    p_read.add_argument("--group", type=int, default=None)
    p_read.add_argument("--index", type=int, default=None)
    p_read.add_argument("--where", default="")

    args = parser.parse_args()

    if args.command == "partitions":
        parts = get_partitions(args.path, args.where)
        for p in parts:
            print(f"{p['channel']}|{p['group']}|{p['index']}|{p['count']}")
    elif args.command == "read":
        path = read_data(args.path, args.channel, args.where, args.group, args.index)
        print(f"PARQUET:{path}")

if __name__ == "__main__":
    main()
