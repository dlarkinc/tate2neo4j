package io.larkin.tate2neo;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Movement {

	private int id;
	
	private String name;
	
	private Era era;

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

	public Era getEra() {
		return era;
	}

	public void setEra(Era era) {
		this.era = era;
	}
}
