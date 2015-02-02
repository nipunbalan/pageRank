package uk.ac.ncl.cs.csc8101.hadoop.calculate;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

public class RankCalculateReducer extends Reducer<Text, Text, Text, Text> {

    /**
     * The `reduce(...)` method is called for each <key, (Iterable of values)> pair in the grouped input.
     * Output values must be of the same type as input values and Input keys must not be altered.
     * <p/>
     * Specifically, this method should take the iterable of links to a page, along with their pagerank and number of links.
     * It should then use these to increase this page's rank by its share of the linking page's:
     * thisPagerank +=  linkingPagerank> / count(linkingPageLinks)
     * <p/>
     * Note: remember pagerank's dampening factor.
     * <p/>
     * Note: Remember that the pagerank calculation MapReduce job will run multiple times, as the pagerank will get
     * more accurate with each iteration. You should preserve each page's list of links.
     *
     * @param page    The individual page whose rank we are trying to capture
     * @param values  The Iterable of other pages which link to this page, along with their pagerank and number of links
     * @param context The Reducer context object, to which key-value pairs are written
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(Text page, Iterable<Text> values, Context context) throws IOException, InterruptedException {


        Text key = page;
        Double rank = 0.0;
        String citedPages = "";
        Boolean firstSpecialCase = true;
        Double dampingFactor = 0.85;
        //   System.out.println(values);
        for (Text valueText : values) {

            String value = valueText.toString();
            String[] strings = value.split("\t");

//            for (String str : strings) {
//                System.out.println("Strings:" + str);
//            }


            //If not special case of source page
            if (!strings[0].equals("!")) {
                if (strings.length > 1 && strings[1] != null && !strings[1].equals("")) {
                    //Getting rank
                    Double pagerank = Double.parseDouble(strings[1]);

                    //Getting number of citations
                    int cites = Integer.parseInt(strings[2]);
                    if (cites != 0) {
                        rank = rank + (pagerank / cites);
                    }
                }

            } else {

                if (strings.length >= 2 && !strings[1].equals("")) {
                    if (!firstSpecialCase) {
                        citedPages = citedPages + ",";
                    }
                    citedPages = citedPages + strings[1];
                }

                //Todo Handle Special case

            }
        }
//        if (citedPages != null && !citedPages.equals("")) {
//            citedPages = " " + citedPages;
//        }


        rank = (1 - dampingFactor) + dampingFactor * (rank);
        DecimalFormat df = new DecimalFormat("###.####");

        Text opValue = new Text("");

        if (citedPages != null && !citedPages.equals("")) {
            opValue = new Text(df.format(rank) + "\t" + citedPages);
        } else {
            opValue = new Text(df.format(rank));
        }


        context.write(key, opValue);
        // System.out.println("(" + key + ",\"" + df.format(rank) + citedPages + "\")");


    }
}
