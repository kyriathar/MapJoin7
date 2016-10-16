import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;


/**
 * Created by hduser on 16/12/2015.
 */
public class InputOptimization {
    //private int[][] multi;
    //private int num_of_files;

    //public InputOptimization(int num_of_files) {
    //    multi = new int[num_of_files][num_of_files];
    //    this.num_of_files = num_of_files;
    //}

    public static int[] Outer(String[] filenames, FileSystem fs, String separator) throws Exception {

        Path pt1, pt2;
        BufferedReader br1, br2;
        String header1, header2;
        String ks;
        int max = -1;
        int counter;
        int[] max_pair = new int[2];
        long multiply_size = -1;
        long multiply_size2;
        for (int i = 0; i < filenames.length; i++) {

            pt1 = new Path(filenames[i]);
            br1 = new BufferedReader(new InputStreamReader(fs.open(pt1)));   //read 1st line of file1
            header1 = br1.readLine();                                       //read header1
	    br1.close();
            for (int j = i + 1; j < filenames.length; j++) {
                pt2 = new Path(filenames[j]);
                br2 = new BufferedReader(new InputStreamReader(fs.open(pt2))); //read 1st line of file2
                header2 = br2.readLine();                                   //read header2
		br2.close();

                ks = StringManipulation.Intersection(header1, header2, separator);
                //split the two keys
                String[] keys = ks.split(";");
                String[] keys_0 = keys[0].split(separator);
                counter = 0;
                for (String str : keys_0) {
                    if (Integer.parseInt(str) != -1) {
                        counter++;
                    }
                }
                if (counter > max) {

                    max = counter;
                    max_pair[0] = i;
                    max_pair[1] = j;
                    multiply_size = (long) (fs.getContentSummary(new Path(filenames[i])).getLength() *
                            fs.getContentSummary(new Path(filenames[j])).getLength());
                } else if (counter == max) {

                    multiply_size2 = (long) (fs.getContentSummary(new Path(filenames[i])).getLength() *
                            fs.getContentSummary(new Path(filenames[j])).getLength());

                    if (multiply_size2 < multiply_size) {
                        max_pair[0] = i;
                        max_pair[1] = j;
                        multiply_size = multiply_size2;
                    }
                }
            }

        }

        return max_pair;
    }

    public static int Inner(String[] filenames, String filename, FileSystem fs, String separator) throws Exception {

        Path pt1, pt2;
        BufferedReader br1, br2;
        String header1, header2;
        String ks;
        int max = -1;
        int counter;
        int max_single = 0;
        long multiply_size = -1;
        long multiply_size2;

        pt1 = new Path(filename + "/" + "part-m-00000");

        br1 = new BufferedReader(new InputStreamReader(fs.open(pt1)));   //read 1st line of file1
        header1 = br1.readLine();                                       //read header1
	br1.close();
        for (int i = 0; i < filenames.length; i++) {
            if (filenames[i] == null)
                continue;

            pt2 = new Path(filenames[i]);
            br2 = new BufferedReader(new InputStreamReader(fs.open(pt2))); //read 1st line of file2
            header2 = br2.readLine();                                   //read header2
	    br2.close();

            ks = StringManipulation.Intersection(header1, header2, separator);
            //split the two keys
            String[] keys = ks.split(";");
            String[] keys_0 = keys[0].split(separator);
            counter = 0;
            for (String str : keys_0) {
                if (Integer.parseInt(str) != -1) {
                    counter++;
                }
            }
            if (counter > max) {

                max = counter;
                max_single =i;
                multiply_size = (long) fs.getContentSummary(new Path(filenames[i])).getLength();
            } else if (counter == max) {

                multiply_size2 = (long) fs.getContentSummary(new Path(filenames[i])).getLength();

                if (multiply_size2 < multiply_size) {
                    max_single = i ;
                    multiply_size = multiply_size2;
                }
            }
        }
        return max_single;
    }
}
