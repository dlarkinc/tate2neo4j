package io.larkin.tate2neo;

import java.util.List;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Artist extends Person {
	
	private int id;
	
	private int birthYear;
	
	private String gender;
	
	private List<Place> activePlaces;
	
	private List<Movement> movements;
	
	private String role;

	public String getRole() {
		return role;
	}

	public void setRole(String role) {
		this.role = role;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	public List<Place> getActivePlaces() {
		return activePlaces;
	}

	public void setActivePlaces(List<Place> activePlaces) {
		this.activePlaces = activePlaces;
	}

	private Birth birth;
	
	public Birth getBirth() {
		return birth;
	}

	public void setBirth(Birth birth) {
		this.birth = birth;
	}

	public List<Movement> getMovements() {
		return movements;
	}

	public void setMovements(List<Movement> movements) {
		this.movements = movements;
	}

	public int getBirthYear() {
		return birthYear;
	}

	public void setBirthYear(int birthYear) {
		this.birthYear = birthYear;
	}

	public String toString() {
		String artist = getName() + " (" + getBirthYear() + ")";
		for (Movement m : getMovements()) {
			artist += "-- " + m.getName() + " (" + m.getEra().getName() + ")";
		}
		artist += "Born: " + getBirth().getTime().getStartYear();
		if (getBirth().getPlace() != null) {
			artist += " (" + getBirth().getPlace().getName() + ")";
		}
		return artist;
	}
}
