package io.larkin.tate2neo.app;

import io.larkin.tate2neo.Artist;
import io.larkin.tate2neo.Artwork;
import io.larkin.tate2neo.Birth;
import io.larkin.tate2neo.Movement;
import io.larkin.tate2neo.Subject;
import io.larkin.tate2neo.config.DefaultConfig;
import io.larkin.tate2neo.repository.ILookupRepository;
import io.larkin.tate2neo.utility.FileFinder;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
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

/**
 * This program imports artists, artworks and their embedded objects / entities, creating
 * connections between the artists, artworks and related entities in a Neo4j database.
 * Because importing into Neo4j using the standard REST and transaction-based means
 * incurs too much latency / overhead, Neo4j's BatchInserter is used to bypass
 * the server and goes directly to files.
 * 
 * @author Larkin.Cunningham
 * 
 * @since December 2014
 *
 */
@Import(DefaultConfig.class)
public class ImportApplication implements CommandLineRunner {

	@Autowired
	private ILookupRepository lookupRepository;

	private final String ARTIST_KEY = "artist:";
	private final String MOVEMENT_KEY = "movement:";
	private final String PLACE_KEY = "place:";
	private final String SUBJECT_KEY = "subject:";
	
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

    /**
     * Initialise the batch inserter
     * 
     * @param dbDir
     */
	private void setupDb(String dbDir) {
		inserter = BatchInserters.inserter(dbDir);
	}
	
	/**
	 * Create the indexes that will be used after the import to improve query
	 * performance.
	 * 
	 */
	private void createIndexes() {
		inserter.createDeferredSchemaIndex(ARTIST).on("name").create();
        inserter.createDeferredSchemaIndex(ARTWORK).on("title").create();
        inserter.createDeferredSchemaIndex(ARTWORK).on("acno").create();
        inserter.createDeferredSchemaIndex(SUBJECT).on("name").create();
        inserter.createDeferredSchemaIndex(MOVEMENT).on("name").create();
        inserter.createDeferredSchemaIndex(PLACE).on("name").create();
    }
	
	/**
	 * Create the subject node using the batch inserter and place the generated
	 * node id in the key-value store.
	 * 
	 * @param subject
	 * @param level	Where in the 3-level subject hierarchy is the subject?
	 * @return A node id that can be used to connect other nodes to the subject
	 */
	private Long addSubjectNode(Subject subject, int level) {
		Map<String, Object> properties = new HashMap<>();
        properties.put("name", subject.getName());
        properties.put("id", subject.getId());
        properties.put("level", level);
        long nodeId = inserter.createNode(properties, SUBJECT);
        
        lookupRepository.add(this.SUBJECT_KEY + subject.getId(), Long.toString(nodeId));
        
        return nodeId;
	}	

	/**
	 * Connect contributors / artists to an artwork. It is assumed the artist has
	 * already been created.
	 * 
	 * @param artworkNode	Physical node id pointing to artwork node
	 * @param contributors	List of contributors / artists who worked on the artwork
	 */
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

	/**
	 * Create an artwork node using the batch inserter
	 * 
	 * @param artwork
	 * @return Physical node id to allow other nodes connect to the artwork
	 */
	private Long createArtworkNode(Artwork artwork) {
        Map<String, Object> properties = new HashMap<>();
        properties.put("title", artwork.getTitle());
        properties.put("id", artwork.getId());
        properties.put("acno", artwork.getAcno());
        long artworkNode = inserter.createNode(properties, ARTWORK);
        return artworkNode;
	}

	/**
	 * Connect an artist to his/her place of birth. If the place node does
	 * not exist, create it using the batch inserter and store the physical
	 * node id in the key-value store.
	 * 
	 * @param artistNode
	 * @param birth an object containing place of birth object and year of birth
	 */
	private void connectArtistToBirthPlace(Long artistNode, Birth birth) {
		// TODO: Refactor to allow non-birth places to be added and connected to
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

	/**
	 * Create an artist node using the batch inserter. Store the physical
	 * node id in the key-value store to allow for other nodes, such as
	 * artworks, to connect to the artist.
	 * 
	 * @param artist
	 * @return Physical node id to allow other nodes connect to an artist
	 */
	private Long createArtistNode(Artist artist) {
        Map<String, Object> properties = new HashMap<>();
        properties.put("name", artist.getMda());
        properties.put("id", artist.getId());
        Long artistNode = inserter.createNode(properties, ARTIST);
        
        // store artist node id in lookup to connect to artworks
        lookupRepository.add(this.ARTIST_KEY + artist.getId(), Long.toString(artistNode));
        
        return artistNode;
	}

	/**
	 * Connect movements to an artist.
	 * 
	 * @param artistNode	Physical node id of artist node
	 * @param movements
	 */
	private void connectArtistToMovements(long artistNode,
			List<Movement> movements) {
        // Connect artists with movements. If necessary, create Movement node(s) and store new node id in lookup
        for (Movement movement : movements) {
        	Long movementNode = getOrCreateMovementNode(movement);
        	// connect artist to movement
        	inserter.createRelationship(artistNode, movementNode, INVOLVED_IN, null);
        }
	}

	/**
	 * Connect movements to artworks.
	 * 
	 * @param artworkNode	Physical node id of artwork node
	 * @param movements
	 */
	private void connectArtworkToMovements(Long artworkNode, List<Movement> movements) {
        if (movements != null) {
	        for (Movement movement : movements) {
	        	Long movementNode = getOrCreateMovementNode(movement);
	        	// connect artwork to movement
	        	inserter.createRelationship(artworkNode, movementNode, PART_OF, null);
	        }
        }
	}

	/**
	 * If a node exists for the provided movement, return the physical node id,
	 * otherwise create the movement and store and return the newly generated id
	 * 
	 * @param movement
	 * @return Physical node id
	 */
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
		
	/**
	 * Get or create top-level subject node
	 * @param subject
	 * @return Physical node id
	 */
	private Long getOrCreateSubjectNode(Subject subject) {
		return getOrCreateSubjectNode(subject, null);
	}
	
	/**
	 * Get or create subject node. If the node doesn't exist, create it.
	 * 
	 * @param subject
	 * @param parent	For 2nd and 3rd level subjects, this will be a non-null physical node id
	 * @return	Physical node id
	 */
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
	
	/**
	 * Given a list of top-level subjects, iterate through them and their child
	 * subjects, connecting the 3rd-level subjects to the privided artwork.
	 * 
	 * @param artworkNode	Physical node id of artwork
	 * @param subjects
	 */
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

	/**
	 * Main import algorithm implemented here. We process artists first so that
	 * we can then link them to the artworks process thereafter.
	 * 
	 * @param args[0]	Neo4j database directory to create
	 * @param args[1]	Directory of artist json files
	 * @param args[2]	Directory of artwork json files
	 */
	@Override
    public void run(String... args) throws Exception {
		
		setupDb(args[0]);

		String artistsDirectory = args[1];
		String artworksDirectory = args[2];
		
		createIndexes();

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
	
	/**
	 * Entry point into application. Delegates to Spring boot command line runner.
	 * 
	 * @param args
	 */
    public static void main(String[] args) {
        SpringApplication.run(ImportApplication.class, args);
    }
}
