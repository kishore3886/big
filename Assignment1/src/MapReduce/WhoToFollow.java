package MapReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.StringTokenizer;
import java.util.function.Predicate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Whom to Follow Implementation using People you may know idea..
 * @author srikanth
 * @author kishore
 */
public class WhoToFollow {


	//Mapper-1
	public static class Mapper1 extends Mapper<Object, Text, IntWritable, IntWritable> {

		public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
			StringTokenizer st = new StringTokenizer(values.toString());
			IntWritable user = new IntWritable(Integer.parseInt(st.nextToken()));

			// Go through the list of all followers of user 'user' and emit (follower,user)
			IntWritable follower = new IntWritable();
			while (st.hasMoreTokens()) {
				Integer friend = Integer.parseInt(st.nextToken());
				follower.set(friend);
				context.write(follower, user);
			}
		}
	}


	//(Identity) Reducer-1
	public static class Reducer1 extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

		// The reduce method
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			IntWritable user = key;

			String valuesString = "";
			while (values.iterator().hasNext()) {
				int value = values.iterator().next().get();
				valuesString+=Integer.valueOf(value)+"\t";
			}
			Text result = new Text(valuesString);
			context.write(user, result);
		}
	}

	//Mapper-2
	public static class Mapper2 extends Mapper<Object, Text, IntWritable, IntWritable> {

		public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
			StringTokenizer st = new StringTokenizer(values.toString());
			IntWritable user = new IntWritable(-Integer.parseInt(st.nextToken()));

			ArrayList<Integer> friends = new ArrayList<Integer>();

			IntWritable friend1 = new IntWritable();
			while (st.hasMoreTokens()) {
				Integer friend = Integer.parseInt(st.nextToken());
				friend1.set(friend);
				context.write(friend1, user);
				friends.add(friend);
			}

			ArrayList<Integer> seenFriends = new ArrayList<Integer>();
			// The element in the pairs that will be emitted.
			IntWritable friend2 = new IntWritable();
			for (Integer friend : friends) {
				friend1.set(friend);
				for (Integer seenFriend : seenFriends) {
					friend2.set(seenFriend);
					context.write(friend1, friend2);
					context.write(friend2, friend1);
				}
				seenFriends.add(friend1.get());
			}
		}
	}

	public static class Reducer2 extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

		// A private class to describe a recommendation.
		// A recommendation has a friend id and a number of friends in common.
		private static class Recommendation {

			// Attributes
			private int friendId;
			private int nCommonFriends;

			// Constructor
			public Recommendation(int friendId) {
				this.friendId = friendId;
				// A recommendation must have at least 1 common friend
				this.nCommonFriends = 1;
			}

			// Getters
			public int getFriendId() {
				return friendId;
			}

			public int getNCommonFriends() {
				return nCommonFriends;
			}

			// Other methods
			// Increments the number of common friends
			public void addCommonFriend() {
				nCommonFriends++;
			}

			// String representation used in the reduce output
			public String toString() {
				return friendId + "(" + nCommonFriends + ")";
			}

			// Finds a representation in an array
			public static Recommendation find(int friendId, ArrayList<Recommendation> recommendations) {
				for (Recommendation p : recommendations) {
					if (p.getFriendId() == friendId) {
						return p;
					}
				}
				// Recommendation was not found!
				return null;
			}
		}

		// The reduce method
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			IntWritable user = key;
			// 'existingFriends' will store the friends of user 'user'
			// (the negative values in 'values').
			ArrayList<Integer> existingFriends = new ArrayList();
			// 'recommendedUsers' will store the list of user ids recommended
			// to user 'user'
			ArrayList<Integer> recommendedUsers = new ArrayList<Integer>();
			while (values.iterator().hasNext()) {
				int value = values.iterator().next().get();
				if (value > 0) {
					recommendedUsers.add(value);
				} else {
					existingFriends.add(value);
				}
			}
			// 'recommendedUsers' now contains all the positive values in 'values'.
			// We need to remove from it every value -x where x is in existingFriends.
			// See javadoc on Predicate: https://docs.oracle.com/javase/8/docs/api/java/util/function/Predicate.html
			for (final Integer friend : existingFriends) {
				recommendedUsers.removeIf(new Predicate<Integer>() {
					//   @Override
					public boolean test(Integer t) {
						return t.intValue() == -friend.intValue();
					}
				});
			}
			
			ArrayList<Recommendation> recommendations = new ArrayList<Recommendation>();
			// Builds the recommendation array
			for (Integer userId : recommendedUsers) {
				Recommendation p = Recommendation.find(userId, recommendations);
				if (p == null) {
					recommendations.add(new Recommendation(userId));
				} else {
					p.addCommonFriend();
				}
			}
			// Sorts the recommendation array
			// See javadoc on Comparator at https://docs.oracle.com/javase/8/docs/api/java/util/Comparator.html
			recommendations.sort(new Comparator<Recommendation>() {
				// @Override
				public int compare(Recommendation t, Recommendation t1) {
					return -Integer.compare(t.getNCommonFriends(), t1.getNCommonFriends());
				}
			});
			// Builds the output string that will be emitted
			StringBuffer sb = new StringBuffer(""); // Using a StringBuffer is more efficient than concatenating strings
			for (int i = 0; i < recommendations.size() && i < 10; i++) {
				Recommendation p = recommendations.get(i);
				sb.append(p.toString() + " ");
			}
			Text result = new Text(sb.toString());
			context.write(user, result);
		}
	}


	//Main
	//Run Config : test.txt temp output
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Who to Follow-1");
		job.setJarByClass(WhoToFollow.class);
		job.setMapperClass(Mapper1.class);
		job.setReducerClass(Reducer1.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		String tempFilePath = args[1];
		FileOutputFormat.setOutputPath(job, new Path(tempFilePath));
		job.waitForCompletion(true);


		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "Who to Follow-2");
		job2.setJarByClass(WhoToFollow.class);
		job2.setMapperClass(Mapper2.class);
		job2.setReducerClass(Reducer2.class);
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job2, new Path(tempFilePath));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}

}
