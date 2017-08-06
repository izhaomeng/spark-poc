package me.zhaomeng.spark;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.wltea.analyzer.lucene.IKAnalyzer;


public class TextAnalyser {
	private static String DEFAULT_FIELD_NAME = "text";
	
	public Map<String, Double> analyzeWithCount(String data) throws IOException {
		Map<String, Double> tf = new HashMap<>();

		try (Analyzer analyzer = new IKAnalyzer()) {
			Reader reader = new StringReader(data);
			TokenStream stream = analyzer.tokenStream(DEFAULT_FIELD_NAME, reader);
			stream.reset();
			while (stream.incrementToken()) {
				String term = stream.getAttribute(CharTermAttribute.class).toString();
				if (tf.containsKey(term)) {
					tf.put(term, tf.get(term) + 1);
				} else {
					tf.put(term, 1D);
				}
			}
		}

		return tf;
	}
	
	public Set<String> analyze(String data) throws IOException {
		Set<String> tf = new HashSet<String>();

		try (Analyzer analyzer = new IKAnalyzer()) {
			Reader reader = new StringReader(data);
			TokenStream stream = analyzer.tokenStream(DEFAULT_FIELD_NAME, reader);
			stream.reset();
			while (stream.incrementToken()) {
				String term = stream.getAttribute(CharTermAttribute.class).toString();
				tf.add(term);
			}
		}

		return tf;
	}
	
}
