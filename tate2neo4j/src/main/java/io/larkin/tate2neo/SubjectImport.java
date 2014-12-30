package io.larkin.tate2neo;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SubjectImport {

	private int id;
	
	private String name;
	
	private String parent0;
	
	private String parent1;

	public String getParent0() {
		return parent0;
	}

	public void setParent0(String parent0) {
		this.parent0 = parent0;
	}

	public String getParent1() {
		return parent1;
	}

	public void setParent1(String parent1) {
		this.parent1 = parent1;
	}

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
	
}
