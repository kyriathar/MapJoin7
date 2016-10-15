import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by hduser on 26/3/2016.
 */

class MyIndex {
    private String name1 ;
    private String name2 ;

    private List<String> firstLine1;
    private List<String> firstLine2;

    private List<Integer> int_list1 ; //px 0,2
    private List<Integer> int_list2 ;  //px 2,0

    private List<String> header ;

    private List<String> header1 ;
    private List<String> header2 ;


    public MyIndex(){
        int_list1 = new ArrayList<>();
        int_list2 = new ArrayList<>();
        name1 = null;
        name2 = null;
        header=null;
        header1 = new ArrayList<>() ;
        header2 = new ArrayList<>() ;
    }

    public String getStr_list1(){
        StringBuilder builder = new StringBuilder();
        for (Integer value : int_list1) {
            builder.append(String.valueOf(value)+"\t");
        }
        String text = builder.toString();
        return text;
    }

    public String getStr_list2(){
        StringBuilder builder = new StringBuilder();
        for (Integer value : int_list2) {
            builder.append(String.valueOf(value)+"\t");
        }
        String text = builder.toString();
        return text;
    }

    public void addList1(int number){
        int_list1.add(number) ;
    }

    public void addList2(int number){
        int_list2.add(number) ;
    }
    /*Key name*/
    public String getName1(){
        return name1;
    }

    public String getName2(){
        return name2;
    }

    public void packName1(String str){
        StringBuilder builder = new StringBuilder();
        builder.append("keyIndex/");
        builder.append(str);
        name1 = builder.toString();
    }

    public void packName2(String str){
        StringBuilder builder = new StringBuilder();
        builder.append("keyIndex/");
        builder.append(str);
        name2 = builder.toString();
    }

    public static String packName(String str, String pack_str){
        StringBuilder builder = new StringBuilder();
        builder.append(pack_str);
        builder.append(str);
        return builder.toString();
    }

    /*header*/
    public void setHeader(List<String> header){
        this.header = header ;
    }

    public List<String> getHeader(){
        return this.header ;
    }

    private void fixHeader(){
        List<String> list = new ArrayList<>();
        for(Integer index : int_list1){
            list.add(firstLine1.get(index));
            firstLine1.set(index,"");
        }
        ArrayList<String> emptyList = new ArrayList<>();
        emptyList.add("");
        firstLine1.removeAll(emptyList);
        list.addAll(firstLine1);
        for(String str : firstLine2){
            if(!list.contains(str)){
                list.add(str);
            }
        }
        this.header = list ;
    }

    public String getHeaderStr(){
        Collections.sort(header);
        fixHeader();
        StringBuilder builder = new StringBuilder();
        for (String value : this.header) {
            builder.append(value);
            builder.append("\t");
        }
        String strHeader = builder.toString();
        return strHeader;
    }

    private String getXs(Path path,Configuration config) throws IOException {     //get first line of file (eg. x1 x2)
        FileSystem fs = FileSystem.get(config);
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(path)));
        String line;
        line=br.readLine();
        return line ;
    }

    public void fixHeaders(Path path1,Path path2,Configuration config,Splitter splitter) throws IOException {
        String myKey1 ,myKey2 ;
        myKey1 = getXs(path1,config);
        myKey2 = getXs(path2,config);
        List<String> list1 = Lists.newArrayList(splitter.split(myKey1));
        List<String> list2 = Lists.newArrayList(splitter.split(myKey2));
        for(Integer i : int_list1 ){
            header1.add(list1.get(i));
        }
        list1.removeAll(header1);
        for(String e : list1){
            header1.add(e);
        }
        for(Integer i : int_list2 ){
            header2.add(list2.get(i));
        }
        list2.removeAll(header2);
        for(String e : list2){
            header2.add(e);
        }
    }

    public static String getHeaderStr2(List<String> header){
        StringBuilder builder = new StringBuilder();
        for (String value : header) {
            builder.append(value);
            builder.append("\t");
        }
        builder.setLength(builder.length() - 1);
        String strHeader = builder.toString();
        return strHeader;
    }


    public List<String> getHeader1() {
        return header1;
    }

    public List<String> getHeader2() {
        return header2;
    }

    /*firstLines*/
    public void setFirstLine1(List<String> firstLine1){
        this.firstLine1 = firstLine1 ;
    }

    public void setFirstLine2(List<String> firstLine2){
        this.firstLine2 = firstLine2 ;
    }
}



public class KeyFinder{
    private Path pt1 ;
    private Path pt2 ;

    public void setPaths(Path pt1, Path pt2 ){
        this.pt1 = pt1 ;
        this.pt2 = pt2 ;
    }

    private String getXs(Path path,Configuration config) throws IOException {     //get first line of file (eg. x1 x2)
        FileSystem fs = FileSystem.get(config);
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(path)));
        String line;
        line=br.readLine();
        return line ;
    }

    public <T> List<T> union(List<T> list1, List<T> list2) {
        Set<T> set = new HashSet<T>();

        set.addAll(list1);
        set.addAll(list2);

        return new ArrayList<T>(set);
    }

    private List<String> findHeader(List<String> list1 ,List<String> list2){
        return union(list1,list2);
    }

    private MyIndex findCommonCols(String s1, String s2, Splitter splitter) {
        if(s1.equals(s2)){
            System.out.println("findCommonCols:: same file ?");
            return null;
        }else{
            MyIndex myIndex = new MyIndex() ;
            List<String> list1 = Lists.newArrayList(splitter.split(s1));
            myIndex.setFirstLine1(list1);
            List<String> list2 = Lists.newArrayList(splitter.split(s2));
            myIndex.setFirstLine2(list2);
            List<String> result_list = new ArrayList<>(list1);
            //System.out.println(list1.toString());
            //System.out.println(result_list.toString());
            //System.out.println(list2.toString());
            result_list.retainAll(list2);       //result_list exei ta koina stoixeia
            Collections.sort(result_list);
            int index1=-1 ;
            int index2=-1 ;
            for(String common_element : result_list){        //gia kathe koino stoixeio
                index1 = list1.indexOf(common_element);
                index2 = list2.indexOf(common_element);
                if(index1>=0)
                    myIndex.addList1(index1);
                if(index2>=0)
                    myIndex.addList2(index2);
                index1 = -1;
                index2 = -1;
            }
            myIndex.setHeader(findHeader(list1,list2));

            return myIndex;
        }
    }

    public MyIndex findKey(Configuration config, Splitter splitter) throws IOException {
        String myKey1 ,myKey2 ;
        myKey1 = getXs(pt1,config);
        myKey2 = getXs(pt2,config);
        return findCommonCols(myKey1,myKey2,splitter);
    }
}