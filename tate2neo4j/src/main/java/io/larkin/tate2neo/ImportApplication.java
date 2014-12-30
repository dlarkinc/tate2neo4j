package io.larkin.tate2neo;

import io.larkin.tate2neo.config.DefaultConfig;
import io.larkin.tate2neo.repository.ILookupRepository;
import io.larkin.tate2neo.utility.FileFinder;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.annotation.Import;

import com.fasterxml.jackson.core.JsonFactory;

@Import(DefaultConfig.class)
public class ImportApplication implements CommandLineRunner{

	@Autowired
	private ILookupRepository lookupRepository;

	private final String ARTIST_KEY = "artist:";
	private final String MOVEMENT_KEY = "movement:";
	private final String PLACE_KEY = "place:";
	private final String SUBJECT_KEY = "subject:";
	
	private final String SUBJECT_LEVEL0_FILE = "C:\\Users\\larkin.cunningham\\git\\collection\\processed\\subjects\\level0list.json";
	private final String SUBJECT_LEVEL1_FILE = "C:\\Users\\larkin.cunningham\\git\\collection\\processed\\subjects\\level1list.json";
	private final String SUBJECT_LEVEL2_FILE = "C:\\Users\\larkin.cunningham\\git\\collection\\processed\\subjects\\level2list.json";

	private BatchInserter inserter;

    private final Label ARTIST = DynamicLabel.label("Artist");
    private final Label ARTWORK = DynamicLabel.label("Artwork");
    private final Label MOVEMENT = DynamicLabel.label("Movement");
    private final Label PLACE = DynamicLabel.label("Place");
    private final Label SUBJECT = DynamicLabel.label("Subject");
    
    private final RelationshipType INVOLVED_IN = DynamicRelationshipType.withName("INVOLVED_IN");
    private final RelationshipType BORN_IN = DynamicRelationshipType.withName("BORN_IN");
    private final RelationshipType CONTRIBUTED_TO = DynamicRelationshipType.withName("CONTRIBUTED_TO");
    private final RelationshipType PART_OF = DynamicRelationshipType.withName("PART_OF");
    private final RelationshipType FEATURES = DynamicRelationshipType.withName("FEATURES");
    private final RelationshipType TYPE_OF = DynamicRelationshipType.withName("TYPE_OF");

	private void setupDb(String dbDir) {
		inserter = BatchInserters.inserter(dbDir);
	}
	
	private void createIndexes() {
		inserter.createDeferredSchemaIndex(ARTIST).on("name").create();
        inserter.createDeferredSchemaIndex(ARTWORK).on("title").create();
        inserter.createDeferredSchemaIndex(ARTWORK).on("acno").create();
	}
	
	private long addSubjectNode(Subject s, int level) {
		Map<String, Object> properties = new HashMap<>();
        properties.put("name", s.getName());
        properties.put("id", s.getId());
        properties.put("level", level);
        long nodeId = inserter.createNode(properties, SUBJECT);
        
        lookupRepository.add(this.SUBJECT_KEY + s.getId(), Long.toString(nodeId));
        
        return nodeId;
	}
	
	@Override
    public void run(String... args) throws Exception {
		
		setupDb(args[0]);

		String artistsDirectory = args[1];
		String artworksDirectory = args[2];
		
		createIndexes();
		      
		// process subject files
/*		
		// Level 0 - top level
		// TODO: Refactor to reuse code among the 3 levels
		List<SubjectImport> subjects = new ObjectMapper().readValue(new File(SUBJECT_LEVEL0_FILE), new TypeReference<List<SubjectImport>>(){});
		for (SubjectImport s : subjects) {
	        long n = addSubjectNode(s, 0);
		}
		
		// Level 1 - 2nd level
		// TODO: Refactor to reuse code among the 3 levels
		subjects = new ObjectMapper().readValue(new File(SUBJECT_LEVEL1_FILE), new TypeReference<List<SubjectImport>>(){});
		for (SubjectImport s : subjects) {
		    long n = addSubjectNode(s, 0);
		    
	        // connect 2nd level node to parent node
	        long parentNode = -1;
        	String value = lookupRepository.get(this.SUBJECT_KEY + s.getParent0());
        	if (value != null && !value.equals("none")) {
        		parentNode = Long.parseLong(value);
        		inserter.createRelationship(levelNode, parentNode, TYPE_OF, null);	
        	}
		}

		// Level 1 - 2nd level
		// TODO: Refactor to reuse code among the 3 levels
		subjects = new ObjectMapper().readValue(new File(SUBJECT_LEVEL2_FILE), new TypeReference<List<SubjectImport>>(){});
		for (SubjectImport s : subjects) {
	        Map<String, Object> properties = new HashMap<>();
	        properties.put("name", s.getName());
	        properties.put("id", s.getId());
	        properties.put("level", 2);
	        long levelNode = inserter.createNode(properties, SUBJECT);
	        
	        // store node id in lookup to connect to artworks
	        lookupRepository.add(this.SUBJECT_KEY + s.getId(), Long.toString(levelNode));
	        
	        // connect 3rd level node to parent node
	        long parentNode = -1;
        	String value = lookupRepository.get(this.SUBJECT_KEY + s.getParent1());
        	if (value != null && !value.equals("none")) {
        		parentNode = Long.parseLong(value);
        		inserter.createRelationship(levelNode, parentNode, TYPE_OF, null);	
        	}
		}
*/
		// process artists
		List<Path> files = FileFinder.getFileList(artistsDirectory, "*.json");
		for (Path f : files) {
			Artist artist = new ObjectMapper().readValue(f.toFile(), Artist.class);
			Long artistNode = createArtistNode(artist);
	        connectArtistToMovements(artistNode, artist.getMovements());
	        connectArtistToBirthPlace(artistNode, artist.getBirth());
		}

		// process artworks
		files = FileFinder.getFileList(artworksDirectory, "*.json");		
		for (Path f : files) {
			Artwork artwork = new ObjectMapper().readValue(f.toFile(), Artwork.class);
			Long artworkNode = createArtworkNode(artwork);			
			try {
				connectArtworkToArtists(artworkNode, artwork.getContributors());
			} catch (Exception e) {
    			System.out.println("Problem connecting artist(s) to " + artwork.getAcno());
    		}	        
	        connectArtworkToMovements(artworkNode, artwork.getMovements());
	        	        
	        // connect subjects with the artwork
	        if (artwork.getSubjects() != null && artwork.getSubjects().getChildren() != null) {
	        	connectArtworkToSubjects(artworkNode, artwork.getSubjects().getChildren());
	        }
		}
		
        inserter.shutdown();
    }

	private void connectArtworkToArtists(Long artworkNode, List<Artist> contributors) {
        for (Artist artist : contributors) {
        	Long cNode = null;
        	String value = lookupRepository.get(this.ARTIST_KEY + artist.getId());
        	if (value != null) {
        		cNode = Long.parseLong(value);
       			inserter.createRelationship(cNode, artworkNode, CONTRIBUTED_TO, null);
        	}
        }
	}

	private Long createArtworkNode(Artwork artwork) {
        Map<String, Object> properties = new HashMap<>();
        properties.put("title", artwork.getTitle());
        properties.put("id", artwork.getId());
        properties.put("acno", artwork.getAcno());
        long artworkNode = inserter.createNode(properties, ARTWORK);
        return artworkNode;
	}

	// TODO: Refactor to allow non-birth places to be added and connected to
	private void connectArtistToBirthPlace(Long artistNode, Birth birth) {
        Long placeNode = null;
        if (birth != null && birth.getPlace() != null) {
	        String placeName = birth.getPlace().getName();
	        if (placeName != null) {
	        	String value = lookupRepository.get(this.PLACE_KEY + placeName);
		        if (value == null) {
	        		HashMap<String, Object> properties = new HashMap<>();
	    	        properties.put("name", placeName);
	    	        placeNode = inserter.createNode(properties, PLACE);
	    	        
	    	        // store new node id in lookup repository
	    	        lookupRepository.add(this.PLACE_KEY + placeName, Long.toString(placeNode));
		        } else {
		        	placeNode = Long.parseLong(value);
		        }
		        
		        // connect artist to birth place
		        HashMap<String, Object> properties = new HashMap<>();
        		if (birth.getTime() != null) {
        			properties.put("startYear", birth.getTime().getStartYear());
        		}
		        inserter.createRelationship(artistNode, placeNode, BORN_IN, properties);
			}
        }
	}

	private Long createArtistNode(Artist artist) {
        Map<String, Object> properties = new HashMap<>();
        properties.put("name", artist.getMda());
        properties.put("id", artist.getId());
        Long artistNode = inserter.createNode(properties, ARTIST);
        
        // store artist node id in lookup to connect to artworks
        lookupRepository.add(this.ARTIST_KEY + artist.getId(), Long.toString(artistNode));
        
        return artistNode;
	}

	private void connectArtistToMovements(long artistNode,
			List<Movement> movements) {
        // Connect artists with movements. If necessary, create Movement node(s) and store new node id in lookup
        for (Movement movement : movements) {
        	Long movementNode = getOrCreateMovementNode(movement);
        	// connect artist to movement
        	inserter.createRelationship(artistNode, movementNode, INVOLVED_IN, null);
        }

		
	}

	private void connectArtworkToMovements(Long artworkNode, List<Movement> movements) {
        if (movements != null) {
	        for (Movement movement : movements) {
	        	Long movementNode = getOrCreateMovementNode(movement);
	        	// connect artwork to movement
	        	inserter.createRelationship(artworkNode, movementNode, PART_OF, null);
	        }
        }
	}

	private Long getOrCreateMovementNode(Movement movement) {
		Long movementNode = null;
		String value = lookupRepository.get(this.MOVEMENT_KEY + movement.getId());
		if (value == null) {
			HashMap<String, Object> properties = new HashMap<>();
	        properties.put("name", movement.getName());
	        properties.put("id", movement.getId());
	        movementNode = inserter.createNode(properties, MOVEMENT);
	        
	        // store new node id in lookup repository
	        lookupRepository.add(this.MOVEMENT_KEY + movement.getId(), Long.toString(movementNode));
		} else {
			movementNode = Long.parseLong(value);
		}
		return movementNode;
	}
		
	private Long getOrCreateSubjectNode(Subject subject) {
		return getOrCreateSubjectNode(subject, null);
	}
	
	private Long getOrCreateSubjectNode(Subject subject, Long parent) {
		Long node = null;
    	String value = lookupRepository.get(this.SUBJECT_KEY + subject.getId());
    	if (value != null) {
    		node = Long.parseLong(value);
    	} else {
    		node = addSubjectNode(subject, 2);
    		if (parent != null) {
    			inserter.createRelationship(node, parent, TYPE_OF, null);
    		}
    	}
    	return node;
	}
	
	private void connectArtworkToSubjects(long artworkNode, List<Subject> subjects) {
		
		// TODO: Refactor to reuse add subject node code for the 3 levels
		
		for (Subject subject0 : subjects) {   // container for subjects
			Long s0Node = getOrCreateSubjectNode(subject0);
        	if (subject0.getChildren() != null) {
	        	for (Subject subject1 : subject0.getChildren()) {   // top level
	        		Long s1Node = getOrCreateSubjectNode(subject1, s0Node);
	        		if (subject0.getChildren() != null) {
		        		for (Subject subject2 : subject1.getChildren()) {    // 2nd level
		        			Long s2Node = getOrCreateSubjectNode(subject2, s1Node);
		        			inserter.createRelationship(artworkNode, s2Node, FEATURES, null);
			        	}	
	        		}
	        	}
        	}
        }
	}
	
    public static void main(String[] args) {
        SpringApplication.run(ImportApplication.class, args);
    }
}
