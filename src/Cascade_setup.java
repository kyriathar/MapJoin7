import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ContentSummary;

public class Cascade_setup {

	// number of files is num_of_iterations + 1
	public static long number_of_iterations(String input_path, FileSystem fs) throws Exception
	{
		Path dir_pt = new Path(input_path);
		ContentSummary cs = fs.getContentSummary(dir_pt);
		long fileCount = cs.getFileCount();
		System.out.println(fileCount + " <---- NUM OF FILES");
		return fileCount - 1;
	}

	public static String[] create_filenames(String input_path, long fileCount) throws Exception
	{
		String[] filenames=new String[(int) fileCount];
		for (int i=0 ;i<fileCount ; i++)
			filenames[i] = input_path + "/" + (i+1);
		return filenames;
		
	}

	public static int compare_size(String file1, String file2, FileSystem fs) throws Exception
	{
		if (fs.getContentSummary(new Path(file1)).getLength() > fs.getContentSummary(new Path(file2)).getLength())
			return 1;
		else
			return 0;
	}


}
