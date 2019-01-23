package hw;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.joda.time.DateTime;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;




/*
 * This program will create a sequence of Longs
 * It will then iterate over them using the IterativeStream
 * It will deduct 1 from each number and filter them to only leaf positive numbers
 * You can watch the number reduce step by step until there will be no more prints, because all numbers are negative
 * */

public class Homework {
	public static void main(String[] args) throws Exception {

		System.out.println("program will calculate which was the most popular resource every day. ");

		ParameterTool params = ParameterTool.fromArgs(args);
		final String inputAug = params.get("input", "access_log_Aug95");
		final int cores = Integer.valueOf(params.get("cores", "4"));
		final int maxEventDelay = 60;       // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second
		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(cores);
		// data generator
		DataStream<String> wordCount = env.readTextFile(inputAug)
				.flatMap(new Tokenizer())
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<Integer, DateTime, String, Integer>>() {
					@Override
					public long extractAscendingTimestamp(Tuple4<Integer, DateTime, String, Integer> element) {
						return element.f1.getMillis();
					}
				})
				.keyBy(2)
				.window(TumblingEventTimeWindows.of(Time.days(1)))
				.sum(3)
				.keyBy(2)
				.windowAll(TumblingEventTimeWindows.of(Time.days(1)))
				.max(3)
				.map(new Mapper());
		wordCount.print();
		env.execute("log processing");
	}


	public static final class Mapper implements MapFunction<Tuple4<Integer, DateTime, String, Integer>, String> {
		@Override
		public String map(Tuple4<Integer, DateTime, String, Integer> t) throws Exception {
			return "day: " + t.f0 + " url: " + t.f2 + " occurrence: " + t.f3;
		}
	}



	public static final class Tokenizer implements FlatMapFunction<String, Tuple4<Integer, DateTime, String, Integer>> {
		private  static  Integer toMonth(String month){
			switch (month){
				case "Jan": return 1;
				case "Feb": return 2;
				case "Mar": return 3;
				case "Apr": return 4;
				case "May": return 5;
				case "Jun": return 6;
				case "Jul": return 7;
				case "Aug": return 8;
				case "Sep": return 9;
				case "Oct": return 10;
				case "Nov": return 11;
				case "Dec": return 12;
			}
			return -1;
		}
		@Override
		public void flatMap(String value, Collector<Tuple4<Integer, DateTime, String, Integer>> out) {
			String[] tokens = value.split(" - - \\[|\\] \"|\\s|\\s\"");

			String[] tmp = tokens[1].split("/|:");
			Integer yyyy = Integer.parseInt(tmp[2]);
			Integer mm = toMonth(tmp[1]);
			Integer day = Integer.parseInt(tmp[0]);
			Integer hour = Integer.parseInt(tmp[3]);
			Integer min = Integer.parseInt(tmp[4]);
			Integer sec = Integer.parseInt(tmp[5]);
			DateTime dt = new DateTime(yyyy, mm, day, 0, 0, 0, 0);

			out.collect(new Tuple4<>(day, dt, tokens[4], 1));
		}
	}
}

