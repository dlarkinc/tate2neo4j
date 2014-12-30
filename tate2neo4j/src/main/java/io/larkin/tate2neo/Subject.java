package io.larkin.tate2neo;

import java.util.List;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Subject {

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<Subject> getChildren() {
		return children;
	}

	public void setChildren(List<Subject> children) {
		this.children = children;
	}

	private int id;
	
	private String name;
	
	private List<Subject> children;
}
