/*
 * Copyright 2017 data Artisans GmbH, 2019 Ververica GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flinktraining.exercises.datastream_java.process;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;

/**
 * The "Long Ride Alerts" exercise of the Flink training
 * (http://training.ververica.com).
 *
 * The goal for this exercise is to emit START events for taxi rides that have not been matched
 * by an END event during the first 2 hours of the ride.
 *
 * Parameters:
 * -input path-to-input-file
 *
 */
public class LongRidesExercise extends ExerciseBase {
	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get("input", ExerciseBase.pathToRideData);

		final int maxEventDelay = 60;       // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		// start the data generator
		DataStream<TaxiRide> rides = env.addSource(rideSourceOrTest(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor)));

		DataStream<TaxiRide> longRides = rides
				.keyBy(ride -> ride.rideId)
				.process(new MatchFunction());

		printOrTest(longRides);

		env.execute("Long Taxi Rides");
	}

	public static class MatchFunction extends KeyedProcessFunction<Long, TaxiRide, TaxiRide> {

		private static final long TWO_HOURS = 2 * 3600 * 1000L;
		private ValueState<TaxiRide> start;
		private ValueState<TaxiRide> end;
		
		@Override
		public void open(Configuration config) throws Exception {
			start = getRuntimeContext().getState(new ValueStateDescriptor<>("startState", TaxiRide.class));
			end = getRuntimeContext().getState(new ValueStateDescriptor<>("endState", TaxiRide.class));
		}

		@Override
		public void processElement(TaxiRide ride, Context context, Collector<TaxiRide> out) throws Exception {
			if (ride.isStart) { //TODO endtime not empty? improvement?
				start.update(ride);
				
				TaxiRide result = end.value();
				if (result == null) {
					context.timerService().registerEventTimeTimer(ride.startTime.getMillis() + TWO_HOURS);
				} else if (result.endTime.getMillis() > ride.startTime.getMillis() + TWO_HOURS) {
					out.collect(ride);
					start.clear();
					end.clear();
					context.timerService().deleteProcessingTimeTimer(ride.startTime.getMillis() + TWO_HOURS);
				}

			} else {
				end.update(ride);
				
				TaxiRide result = start.value();
				if (result == null) {
					// wait for start ride event
				} else {
					if ((ride.endTime.getMillis() - result.startTime.getMillis()) > TWO_HOURS) {
						out.collect(result);
						start.clear();
						end.clear();
						context.timerService().deleteProcessingTimeTimer(ride.startTime.getMillis() + TWO_HOURS);
					}
			    }
			}
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext context, Collector<TaxiRide> out) throws Exception {
			TaxiRide resultStart = start.value();
	        if (resultStart != null && end.value() == null) {
	        	out.collect(resultStart);
	        	start.clear();
	        	end.clear();
	        }
		}
	}
	
}