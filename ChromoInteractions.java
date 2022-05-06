import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class ChromoInteractions {
	/**
	 * method name : setCeiling
	 * description : Add <Chromosome, no. of base pairs> in a HashMap as key,value pairs
	 * @return : HashMap<Integer, Integer>
	 */

	public static HashMap<Integer,Integer> setCeiling() {
		HashMap<Integer, Integer> ceilingMap = new HashMap<Integer,Integer>();
		ceilingMap.put(1,248956422);
		ceilingMap.put(2,242193529);
		ceilingMap.put(3,198295559 );
		ceilingMap.put(4,190214555 );
		ceilingMap.put(5,181538259 );
		ceilingMap.put(6, 170805979);
		ceilingMap.put(7, 159345973);
		ceilingMap.put(8, 145138636);
		ceilingMap.put(9, 138394717);
		ceilingMap.put(10, 133797422);
		ceilingMap.put(11, 135086622);
		ceilingMap.put(12, 133275309);
		ceilingMap.put(13, 114364328);
		ceilingMap.put(14,107043718 );
		ceilingMap.put(15, 101991189);
		ceilingMap.put(16, 90338345);
		ceilingMap.put(17,83257441 );
		ceilingMap.put(18, 80373285);
		ceilingMap.put(19, 58617616);
		ceilingMap.put(20, 64444167);
		ceilingMap.put(21, 46709983);
		ceilingMap.put(22,50818468 );
		ceilingMap.put(23, 156040895);
		return ceilingMap;
	}
	/**
	 * method name : allocateBins
	 * description: Allocates bins to all chromosomes, keeping 100000 base pairs in one
	 * @param : hMap
	 * @return : HashMap<Integer, Integer>
	 */
	public static HashMap<Integer,Integer> allocateBins(HashMap<Integer,Integer> hMap){
		HashMap<Integer, Integer> binMap = new HashMap<Integer,Integer>();
		for (Map.Entry<Integer, Integer> entry: hMap.entrySet()) {
			binMap.put(entry.getKey(), entry.getValue()/100000+1);
		}
		return binMap;
	}
	/**
	 * method name : calculateBinPairs
	 * description : It takes data from input file one row at a time and returns bin pair for that interaction if it is valid
	 * @param row
	 * @param binsMap
	 * @param cMap
	 * @return String (bin pair)
	 */

	public static String calculateBinPairs(String row, HashMap<Integer,Integer> binsMap, HashMap<Integer,Integer> ceilMap) {
		String[] rowArr = row.split("\\s+");
		String chromoOne = rowArr[0];
		String chromoTwo = rowArr[3];
		int tempValChromoOne = Integer.parseInt(rowArr[1])/100000 + 1;
		int tempValChromoTwo = Integer.parseInt(rowArr[4])/100000 + 1;
		int binValOne= tempValChromoOne;
		int binValTwo=tempValChromoTwo;
		boolean invalidBasePairOne = Integer.parseInt(rowArr[1])> ceilMap.get(Integer.parseInt(rowArr[0]));

		boolean invalidBasePairTwo = Integer.parseInt(rowArr[4])> ceilMap.get(Integer.parseInt(rowArr[3]));


		if(!invalidBasePairOne && !invalidBasePairTwo) {
			if(!chromoOne.equalsIgnoreCase("1")) {
				for(int i=1; i<Integer.parseInt(chromoOne);i++) {
					binValOne+= binsMap.get(i);
				}

			}

			if(!chromoTwo.equalsIgnoreCase("1")) {
				for(int i=1; i<Integer.parseInt(chromoTwo);i++) {
					binValTwo+= binsMap.get(i);
				}

			}

			if(Integer.parseInt(chromoOne)>Integer.parseInt(chromoTwo)) {
				return binValTwo+","+binValOne;
			}
			else {
				return binValOne+","+binValTwo;
			}


		}

			else {
				return "invalid";
			}

	}



	// Mapper Class
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
		HashMap<Integer,Integer> hMap = setCeiling();
		HashMap<Integer,Integer> binMap = allocateBins(hMap);
		private final static IntWritable one = new IntWritable(1);
	    private Text word ;

	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    	String [] rowsArr = value.toString().split("\n");
	    	for(String s: rowsArr) {
	    		String pair = calculateBinPairs(s,binMap, hMap);
	    		if(!pair.equalsIgnoreCase("invalid")) {
	    			word = new Text(pair);
			    	context.write(word, one);
	    		}



	    	}

	    }
	}
	// Reducer class
	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
	    }
	}

	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Chromosome Interactions");
	    job.setJarByClass(ChromoInteractions.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }


}

