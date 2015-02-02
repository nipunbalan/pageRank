import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import uk.ac.ncl.cs.csc8101.hadoop.parse.PageLinksParseMapper;
import uk.ac.ncl.cs.csc8101.hadoop.parse.PageLinksParseReducer;

import java.util.ArrayList;
import java.util.List;



/**
 * Created by nipun on 1/31/2015.
 */
public class MRTester {

    MapDriver<LongWritable, Text, Text, Text> mapDriver;
    ReduceDriver<Text, Text, Text, Text> reduceDriver;
   // MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;


    @Before
    public void setUp()
    {
        PageLinksParseMapper parseMapper = new PageLinksParseMapper();
        PageLinksParseReducer parseReducer = new PageLinksParseReducer();
        mapDriver = MapDriver.newMapDriver(parseMapper);
        reduceDriver = ReduceDriver.newReduceDriver(parseReducer);
    }

    @Test
    public void testParseMapper() throws Exception{

        String ipString = "<mediawiki>\n" +
                "    <page>\n" +
                "        <title>Foo</title>\n" +
                "        <text>Lorem ipsum dolor sit amet...</text>\n" +
                "    </page>\n" +
                "    <page>\n" +
                "        <title>Bar</title>\n" +
                "        <text>Lorem ipsum dolor sit [[Foo|amet]]...</text>\n" +
                "    </page>\n" +
                "        <page>\n" +
                "        <title>Baz</title>\n" +
                "        <text>[[Foo]] [[Bar]] not Lipsum...</text>\n" +
                "    </page>\n" +
                "    <page>...<page>\n" +
                "</mediawiki>";
        mapDriver.withInput(new LongWritable(), new Text(ipString));
        List<Pair<Text, Text>> result = mapDriver.run();
        System.out.println("testParseMapper Result:"+result);

  //      mapDriver.withOutput(new Text("Foo"), new Text("li"));
//        mapDriver.runTest();
       // mapDriver.withInput(new LongWritable(),new Text(ipString));
       // mapDriver.withOutput(new Text("Bar"),new Text("Foo"));
       // mapDriver.runTest();
    }
@Test
    public void testParseMapper2() throws Exception{

        String ipString = "<mediawiki>\n" +
                "        <page>\n" +
                "        <title>Baz</title>\n" +
                "        <text>[[Foo]] [[Bar]] [[hg]]not Lipsum...</text>\n" +
                "    </page>\n" +
                "    <page>...<page>\n" +
                "</mediawiki>";
        mapDriver.withInput(new LongWritable(), new Text(ipString));
       // mapDriver.withOutput(new Text("Baz"), new Text("Bar"));
     List<Pair<Text, Text>> result = mapDriver.run();
    System.out.println("testParseMapper2 Result:"+result);
   // assertThat(result).contains("Baz");
       // mapDriver.runTest();
        // mapDriver.withInput(new LongWritable(),new Text(ipString));
        // mapDriver.withOutput(new Text("Bar"),new Text("Foo"));
        // mapDriver.runTest();
    }

    @Test
    public void testParseReducer()throws Exception{
        List<Text> valueList = new ArrayList<>();
        valueList.add(new Text("Foo"));
        valueList.add(new Text("Bar"));
        reduceDriver.withInput(new Text("Baz"),valueList);
        List<Pair<Text, Text>> result = reduceDriver.run();
        System.out.println("testParseReducer Result:"+result);


    }


}
