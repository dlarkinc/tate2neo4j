package io.larkin.tate2neo;

import java.util.List;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Artwork {

	private int id;
	
	private String acno;
	
	private String title;
	
	private String classification;
	
	private String medium;
	
	private List<Artist> contributors;
	
	private List<Movement> movements;
	
	private CatalogueGroup catalogueGroup;

	private Subject subjects;
	
	public String getMedium() {
		return medium;
	}

	public void setMedium(String medium) {
		this.medium = medium;
	}

	public String getClassification() {
		return classification;
	}

	public void setClassification(String classification) {
		this.classification = classification;
	}

	public CatalogueGroup getCatalogueGroup() {
		return catalogueGroup;
	}

	public void setCatalogueGroup(CatalogueGroup catalogueGroup) {
		this.catalogueGroup = catalogueGroup;
	}

	public Subject getSubjects() {
		return subjects;
	}

	public void setSubjects(Subject subjects) {
		this.subjects = subjects;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getAcno() {
		return acno;
	}

	public void setAcno(String acno) {
		this.acno = acno;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public List<Artist> getContributors() {
		return contributors;
	}

	public void setContributors(List<Artist> contributors) {
		this.contributors = contributors;
	}

	public List<Movement> getMovements() {
		return movements;
	}

	public void setMovements(List<Movement> movements) {
		this.movements = movements;
	}
}
