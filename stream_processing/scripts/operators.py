# Please see other transformations here
# https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/operators/overview/
from pyflink.datastream import StreamExecutionEnvironment


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # Default to number of cores

    # Map
    print("=" * 35 + "MAP" + "=" * 35)
    data_stream = env.from_collection([1, 2, 3, 4, 5])
    data_stream.map(lambda x: 2 * x).print()
    env.execute()

    # Filter
    print("=" * 35 + "FILTER" + "=" * 35)
    data_stream = env.from_collection([1, 2, 3, 4, 5])
    data_stream.filter(lambda x: x % 2 == 0).print()
    env.execute()

    # FlatMap
    print("=" * 35 + "FLATMAP" + "=" * 35)
    data_stream = env.from_collection(["This is", "an example."])
    data_stream.flat_map(lambda x: x.split(" ")).print()
    env.execute()

    # Reduce
    print("=" * 35 + "REDUCE" + "=" * 35)
    data_stream = env.from_collection(
        collection=[("1st_word", 1), ("2nd_word", 1), ("1st_word", 1)]
    )
    data_stream.key_by(lambda x: x[0]).reduce(lambda a, b: (a[0], a[1] + b[1])).print()
    env.execute()

    # Union
    print("=" * 35 + "UNION" + "=" * 35)
    data_stream_1 = env.from_collection([1, 2, 3, 4, 5])
    data_stream_2 = env.from_collection([6, 7, 8, 9, 10])
    data_stream_1.union(data_stream_2).print()
    env.execute()


if __name__ == "__main__":
    main()
