package idv.jhuang.sql4j;

import idv.jhuang.sql4j.Configuration.Type;

import java.util.LinkedHashMap;

@SuppressWarnings("serial")
public class Selection extends LinkedHashMap<String, Selection> {
	private final Type type;
	
	public Selection(Type type, String selectionStr) {
		super();
		this.type = type;
	}
	
}
