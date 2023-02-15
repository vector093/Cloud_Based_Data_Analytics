package k_means_2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class KmeansDriver {
    public static List<DataPoints> initial_centers,new_centers;
    private static KmeansFile util;
    
    public static class KmeansMapper extends Mapper<LongWritable, Text,DataPoints,DataPoints> {
        private List<DataPoints> centers=null;

        //Reading Cluster centers from HDFS file we've written
        public void setup(Context context) throws IOException {
            this.centers=KmeansFile.readFromHDFS("K_means/read");
        }

        public void map(LongWritable key, Text value, Context context){
            DataPoints v=new DataPoints(value.toString());
            DataPoints nearestCenter=null;
            double minD=Double.MAX_VALUE;
            // calculating euclidean distance for all the points from the 4 cluster centers
            for(DataPoints center:centers){
                double d= Math.sqrt((center.getX()-v.getX())*(center.getX()-v.getX())+
                        (center.getY()-v.getY())*(center.getY()-v.getY()));
                //check the which center is the closest to the point and assign that center as the key to that point
                if(nearestCenter==null){
                    nearestCenter=center;
                    minD=d;
                }else{
                    if(minD>d){
                        nearestCenter=center;
                        minD=d;
                    }
                }
            }
            try {
                context.write(nearestCenter,v);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    
    
    public static class KmeansReducer extends Reducer<DataPoints,DataPoints, NullWritable,DataPoints> {
        public Integer id=0;
        public void reduce(DataPoints key,Iterable<DataPoints> values,Context context){
            //new datapoints are created to store the new centers
        	DataPoints newCenter=new DataPoints();
            int count=0;
            // all the x and y values of the points with the same center are added together
            for(DataPoints p:values){
                count++;
                newCenter.add(p);
            }
            // the summed x and y values are divided by the total number of points that were added together to give use the new centers
            newCenter.divide(count);
            //id from 1-4 is set for the 4 centers
            newCenter.setId(++id);
            try {
                context.write(NullWritable.get(),newCenter);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    	System.out.println("K_means started!");
        initial_centers=new ArrayList<>();
        
        //Initialize the 4 cluster centers from a local txt file
        initial_centers=KmeansFile.initilize("E:\\University of Toronto\\Subjects\\big data\\assignment 1\\centroids.txt");  
        
        //print the centers to console
        System.out.println("initial centroids-");
        System.out.println(initial_centers.toString());
        
        //writing the center into a hdfs file
        KmeansFile.writeToHDFS(initial_centers,"K_means/read");  
        //current_centroids=initial_centroids;    //Save the initialization cluster center to the current cluster center
        
        int iterators=1;  
        
        while (true){
        	if(iterators>15) //Stop running more than 15 iterations
    		{
    		System.out.println("Max iteration has been reached");
    		break;
    		}
        	
        	System.out.println("the iteration is - "+iterators);//the iteration number
        	
        	//running the main mapreduce
            run(args[0], args[1]);
            
            //Writing the output centers from map reduce back into the input directory so that the next iteration can read them and use it
            KmeansFile.reWriteHDFS("K_means/Output/part-r-00000","K_means/read");
            
            //printing the centers for every iteration
            new_centers=KmeansFile.readFromHDFS("K_means/read");
            System.out.println("newCentorids is "+new_centers.toString());
            
            iterators++;            
        }
        //after max iteration, print result to console as the cat command cannot read a file which has been written with sequence writer
        List<DataPoints> result=KmeansFile.readFromHDFS("K_means/read");
        System.out.println("the result is :");
        for(int i=0;i<result.size();i++)
        {
            System.out.println(result.get(i).getX()+" , "+result.get(i).getY());
        }
    }

    public static void run(String input_path, String output_path) throws IOException, ClassNotFoundException, InterruptedException {
    	Configuration conf=new Configuration();
    	
    	//Deleting the old output directory from hdfs so that a new one can take its place
    	FileSystem fs = FileSystem.get(conf);
    	 Path path = new Path("K_means/Output");
    	if(fs.exists(path)) {
    	fs.delete(path, true);}
        
    	//Configuration of mapreduce
        Job job=new Job(conf,"Kmeans");
        job.setJarByClass(KmeansDriver.class);
        job.setMapperClass(KmeansMapper.class);
        job.setReducerClass(KmeansReducer.class);
        job.setMapOutputKeyClass(DataPoints.class);
        job.setMapOutputValueClass(DataPoints.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(DataPoints.class);
        
        //setting the input and output file paths
        FileInputFormat.addInputPath(job,new Path(input_path));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job,new Path(output_path));
        int code=job.waitForCompletion(true)?0:1;
    }
    
    
public static class KmeansFile {
    	
    	
        //Initialize the cluster center from the file path
        public static List<DataPoints> initilize(String p) throws IOException {
            BufferedReader br=new BufferedReader(new FileReader(p));
            List<DataPoints> point=new ArrayList<>();
            String str;
            int t=0;
            while ((str=br.readLine())!=null&&t<4){

                point.add(new DataPoints(str));
                t++;
            }
            br.close();
            return point;
        }
        
        
        //Write the cluster center to the file path
        public static void writeToHDFS(List<DataPoints> point,String p) throws IOException {
            Configuration conf=new Configuration();
            FileSystem fs=FileSystem.get(URI.create(p),conf);
            Path path=new Path(p);

            int t=0;
            NullWritable key= NullWritable.get();
            DataPoints value=new DataPoints();
            SequenceFile.Writer writer=null;
            try{
                writer=SequenceFile.createWriter(fs,conf,path,key.getClass(),value.getClass());
                for(int i=0;i<point.size();i++){
                    t++;
                    point.get(i).setId(t);
                    value=new DataPoints(point.get(i).toString());
                    writer.append(key,value);
                }
            }catch (NullPointerException e){
                e.printStackTrace();
            } finally {
                IOUtils.closeStream(writer);
            }
        }
        
        
        //Reading Cluster Center from the file path 
        public static List<DataPoints> readFromHDFS(String p) throws IOException {
            Configuration conf=new Configuration();
            FileSystem fs=FileSystem.get(URI.create(p),conf);
            Path path=new Path(p);
            List<DataPoints> points = new ArrayList<>();
            SequenceFile.Reader reader=null;
            try{
                reader=new SequenceFile.Reader(fs,path,conf);
                Writable key=(Writable) ReflectionUtils.newInstance(reader.getKeyClass(),conf);
                Writable value=(Writable) ReflectionUtils.newInstance(reader.getValueClass(),conf);
                long positon=reader.getPosition();

                while(reader.next(key,value)){
                    DataPoints test=new DataPoints(value.toString());
                    points.add(test);
                    positon=reader.getPosition();
                }
            }finally {
                IOUtils.closeStream(reader);
            }
            return points;
        }

        
        //Write the new cluster center generated by MapReduce calculation into the file system for next reading
        public static void reWriteHDFS(String from,String to) throws IOException {
            List<DataPoints> points=readFromHDFS(from);
            writeToHDFS(points,to);
        }
    }
    

}
