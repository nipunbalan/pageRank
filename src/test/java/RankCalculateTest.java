import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import uk.ac.ncl.cs.csc8101.hadoop.calculate.RankCalculateMapper;
import uk.ac.ncl.cs.csc8101.hadoop.calculate.RankCalculateReducer;
import uk.ac.ncl.cs.csc8101.hadoop.parse.PageLinksParseMapper;
import uk.ac.ncl.cs.csc8101.hadoop.parse.PageLinksParseReducer;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by nipun on 2/1/2015.
 */
public class RankCalculateTest {
    MapDriver<LongWritable, Text, Text, Text> mapDriver;
    ReduceDriver<Text, Text, Text, Text> reduceDriver;


    @Before
    public void setUp()
    {
        RankCalculateMapper rankCalculateMapper = new RankCalculateMapper();
        RankCalculateReducer rankCalculateReducer = new RankCalculateReducer();
        mapDriver = MapDriver.newMapDriver(rankCalculateMapper);
        reduceDriver = ReduceDriver.newReduceDriver(rankCalculateReducer);
    }

    @Test
    public void testRankCalculateMapper() throws Exception{

        String ipString = "Baz\t1.0\tFoo,Bar";

      //  String ipString = "A\t0.57";
        mapDriver.withInput(new LongWritable(), new Text(ipString));
        List<Pair<Text, Text>> result = mapDriver.run();
        System.out.println("testRankCalculateMapper Result:"+result);
    }


    @Test
    public void testRankCalculateReducer() throws Exception{

        List<Text> valueList = new ArrayList<>();
        Text key= new Text("Baz");
    valueList.add(new Text("!\tFoo,Bar"));
       // valueList.add(new Text("!\tFoo"));
        reduceDriver.withInput(key, valueList);
        List<Pair<Text, Text>> result = reduceDriver.run();
        System.out.println("testRankCalculateReducer Result:"+result);

    }
}
