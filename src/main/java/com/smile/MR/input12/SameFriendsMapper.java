package com.smile.MR.input12;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

/**
 * @author smi1e
 * Date 2019/10/5 17:19
 * Description
 */
public class SameFriendsMapper extends Mapper<Text, Text, Text, Text> {
    private Text k = new Text();
    private Text v = new Text();
    private LinkedHashMap<String, ArrayList<String>> map = new LinkedHashMap<>();
    private LinkedList<String> people = new LinkedList<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath();
        BufferedReader bf = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
        String line;
        while (StringUtils.isNotEmpty(line = bf.readLine())) {
            String[] fields = line.split(":");
            people.add(fields[0]);
            String[] friends = fields[1].split(",");
            ArrayList<String> f = new ArrayList<>();
            for (String friend : friends) {
                f.add(friend);
            }
            map.put(fields[0], f);
        }
        IOUtils.closeStream(bf);
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String A = key.toString();
        ArrayList<String> Afriends = map.get(A);
        int i = people.indexOf(A);
        ListIterator<String> stringListIterator = people.listIterator(i);
        while (stringListIterator.hasNext()) {
            String B = stringListIterator.next();
            if (A.equals(B)) {
                continue;
            } else {
                k.set(A + "-" + B);
            }
            ArrayList<String> Bfriends = map.get(B);
            Afriends.forEach(x -> {
                if (Bfriends.contains(x)) {
                    v.set(x);
                    try {
                        context.write(k, v);
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }
}
