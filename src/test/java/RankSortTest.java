import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import uk.ac.ncl.cs.csc8101.hadoop.rank.RankSortMapper;

import java.util.List;

/**
 * Created by nipun on 2/2/2015.
 */
public class RankSortTest {

    MapDriver<LongWritable, Text, FloatWritable, Text> mapDriver;


    @Before
    public void setUp()
    {
        RankSortMapper rankSortMapper = new RankSortMapper();
        mapDriver = MapDriver.newMapDriver(rankSortMapper);
    }

    @Test
    public void testRankSortMapper() throws Exception{

        String ipString = "Baz\t0.15\tFoo,Bar";
        mapDriver.withInput(new LongWritable(), new Text(ipString));
        List<Pair<FloatWritable,Text>> result = mapDriver.run();
        System.out.println("testRankCalculateMapper Result:"+result);
    }



}
