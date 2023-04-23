package org.example.flink.bean;

/**
 * @ClassName WordCount
 * @Author wangyingkang
 * @Date 2022/1/18 10:41
 * @Version 1.0
 * @Description 单词计频实体类
 **/
public class WordCount {

    private String wordName;

    private long frequency;

    public WordCount() {
    }

    public WordCount(String wordName, long frequency) {
        this.wordName = wordName;
        this.frequency = frequency;
    }

    public String getWordName() {
        return wordName;
    }

    public void setWordName(String wordName) {
        this.wordName = wordName;
    }

    public long getFrequency() {
        return frequency;
    }

    public void setFrequency(long frequency) {
        this.frequency = frequency;
    }

    @Override
    public String toString() {
        return "WordCount{" +
                "wordName='" + wordName + '\'' +
                ", frequency=" + frequency +
                '}';
    }
}
