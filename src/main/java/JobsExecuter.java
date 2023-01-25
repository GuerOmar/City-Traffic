

import org.apache.hadoop.util.ToolRunner;

public class JobsExecuter {
    public static void main(String[] args) throws Exception {
        String[] multieArgs = {args[0], "city_traffic_data"};
        int exitCode =ToolRunner.run(new MultiMapperDriver(), multieArgs);
        String[] testArgs = {"city_traffic_data/part-r-00000","hour_reducer_data"};
        int exitCode2 =ToolRunner.run(new ReducerAdaptaterDriver(HourReducer.class), testArgs);
        testArgs[1]="day_reducer_data";
        int exitCode3 =ToolRunner.run(new ReducerAdaptaterDriver(DayReducer.class), testArgs);
        testArgs[1]="vehicule_reducer_data";
        int exitCode4 =ToolRunner.run(new ReducerAdaptaterDriver(VehicleReducer.class), testArgs);
        testArgs[1]="sensor_reducer_data";
        int exitCode5 =ToolRunner.run(new ReducerAdaptaterDriver(SensorReducer.class), testArgs);

        testArgs[0]="hour_reducer_data/part-r-00000";
        int exitCode6 =ToolRunner.run(new WriteToHBaseAdaptater(WriteDayHour.class,"oguermazi:hour_reducer_data"), testArgs);

        testArgs[0]="day_reducer_data/part-r-00000";
        int exitCode7 =ToolRunner.run(new WriteToHBaseAdaptater(WriteDay.class,"oguermazi:day_reducer_data"), testArgs);

        testArgs[0]="vehicule_reducer_data/part-r-00000";
        int exitCode8 =ToolRunner.run(new WriteToHBaseAdaptater(WriteVehicule.class,"oguermazi:vehicule_reducer_data"), testArgs);

        testArgs[0]="sensor_reducer_data/part-r-00000";
        int exitCode9 =ToolRunner.run(new WriteToHBaseAdaptater(WriteSensor.class,"oguermazi:sensor_reducer_data"), testArgs);

        System.exit(exitCode);
    }
}
