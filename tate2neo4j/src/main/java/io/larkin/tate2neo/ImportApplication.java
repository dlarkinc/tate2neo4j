package io.larkin.tate2neo;

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
	private final String PERSON_BY_NAME_KEY = "person_by_name:";
	private final String CATALOGUE_GROUP_KEY = "catalogue_group:";
	private final String CLASSIFICATION_KEY = "classification:";
	private final String MEDIUM_KEY = "medium:";
	private final String MOVEMENT_KEY = "movement:";
	private final String PLACE_KEY = "place:";
	private final String SUBJECT_KEY = "subject:";
	
	private BatchInserter inserter;

    private final Label ARTWORK = DynamicLabel.label("Artwork");
    private final Label CATALOGUE_GROUP = DynamicLabel.label("CatalogueGroup");
    private final Label CLASSIFICATION = DynamicLabel.label("Classification");
    private final Label MEDIUM = DynamicLabel.label("Medium");
    private final Label MOVEMENT = DynamicLabel.label("Movement");
    private final Label PERSON = DynamicLabel.label("Person");
    private final Label PLACE = DynamicLabel.label("Place");
    private final Label SUBJECT = DynamicLabel.label("Subject");
    
    private final RelationshipType BELONGS_TO = DynamicRelationshipType.withName("BELONGS_TO");
    private final RelationshipType BORN_IN = DynamicRelationshipType.withName("BORN_IN");
    private final RelationshipType CLASSIFIED_AS = DynamicRelationshipType.withName("CLASSIFIED_AS");
    private final RelationshipType COMPRISED_OF = DynamicRelationshipType.withName("COMPRISED_OF");
    private final RelationshipType CONTRIBUTED_TO = DynamicRelationshipType.withName("CONTRIBUTED_TO");
    private final RelationshipType FEATURES = DynamicRelationshipType.withName("FEATURES");
    private final RelationshipType INVOLVED_IN = DynamicRelationshipType.withName("INVOLVED_IN");
    private final RelationshipType PART_OF = DynamicRelationshipType.withName("PART_OF");
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
		inserter.createDeferredSchemaIndex(PERSON).on("name").create();
        inserter.createDeferredSchemaIndex(ARTWORK).on("title").create();
        inserter.createDeferredSchemaIndex(ARTWORK).on("acno").create();
        inserter.createDeferredSchemaIndex(SUBJECT).on("name").create();
        inserter.createDeferredSchemaIndex(MOVEMENT).on("name").create();
        inserter.createDeferredSchemaIndex(PLACE).on("name").create();
    }
	
	/**
	 * Create an artist node using the batch inserter. Store the physical
	 * node id in the key-value store to allow for other nodes, such as
	 * artworks, to connect to the artist.
	 * 
	 * @param artist
	 * @return Physical node id to allow other nodes connect to an artist
	 */
	private Long addArtistNode(Artist artist) {
        Map<String, Object> properties = new HashMap<>();
        properties.put("name", artist.getName());
        properties.put("id", artist.getId());
        Long artistNode = inserter.createNode(properties, PERSON);
        
        // store artist node id in lookup to connect to artworks
        lookupRepository.add(this.ARTIST_KEY + artist.getId(), Long.toString(artistNode));
        
        // store artist name in lookup to match against subjects
        lookupRepository.add(this.PERSON_BY_NAME_KEY + artist.getName(), Long.toString(artistNode));
        
        return artistNode;
	}

	/**
	 * Create an artwork node using the batch inserter
	 * 
	 * @param artwork
	 * @return Physical node id to allow other nodes connect to the artwork
	 */
	private Long addArtworkNode(Artwork artwork) {
        Map<String, Object> properties = new HashMap<>();
        properties.put("title", artwork.getTitle());
        properties.put("id", artwork.getId());
        properties.put("acno", artwork.getAcno());
        long artworkNode = inserter.createNode(properties, ARTWORK);
        return artworkNode;
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
		return getOrCreateSubjectNode(subject, null, null);
	}
	
	/**
	 * Get or create subject node. If the node doesn't exist, create it. There can be special cases
	 * of subject, such as named persons or places. These will still be added to the subject hierarchy,
	 * but with more appropriate labels.
	 * 
	 * @param subject
	 * @param parent	For 2nd and 3rd level subjects, this will be a non-null physical node id
	 * @param parentObject The nature of the parent may alter the node label
	 * @return	Physical node id
	 */
	private Long getOrCreateSubjectNode(Subject subject, Long parent, Subject parentObject) {
		Long node = null;
    	String value = lookupRepository.get(this.SUBJECT_KEY + subject.getId());
    	if (value != null) {
    		node = Long.parseLong(value);
    	} else {
    		node = addSubjectNode(subject, parentObject);
    		if (parent != null) {
    			inserter.createRelationship(node, parent, TYPE_OF, null);
    		}
    	}
    	return node;
	}
	
	/**
	 * Create the subject node using the batch inserter and place the generated
	 * node id in the key-value store.
	 * 
	 * @param subject
	 * @param parentObject	Where in the 3-level subject hierarchy is the subject?
	 * @return A node id that can be used to connect other nodes to the subject
	 */
	private Long addSubjectNode(Subject subject, Subject parentObject) {
		Map<String, Object> properties = new HashMap<>();
        Label sLabel = SUBJECT;
        
        if (parentObject != null && parentObject.isNamedIndividuals()) {
        	sLabel = PERSON;
        	properties.put("name", subject.getName());
        	
        	// check artist nodes to see if person already exists
        	String value = lookupRepository.get(this.PERSON_BY_NAME_KEY + subject.getName());
        	if (value != null) {
        		return Long.parseLong(value);
        	} else {
        		Long nodeId = inserter.createNode(properties, sLabel);
                // store artist name in lookup to match against other subjects
                lookupRepository.add(this.PERSON_BY_NAME_KEY + subject.getName(), Long.toString(nodeId));
                return nodeId;
        	}
        } else {
        	properties.put("name", subject.getName());
	        properties.put("id", subject.getId());
        }
        
        Long nodeId = inserter.createNode(properties, sLabel);
        
        lookupRepository.add(this.SUBJECT_KEY + subject.getId(), Long.toString(nodeId));
        
        return nodeId;
	}	
	
	/**
	 * Given a list of top-level subjects, iterate through them and their child
	 * subjects, connecting the 3rd-level subjects to the privided artwork.
	 * 
	 * @param artworkNode	Physical node id of artwork
	 * @param subjects
	 */
	private void connectArtworkToSubjects(long artworkNode, List<Subject> subjects) {
		for (Subject subject0 : subjects) {   // container for subjects
			Long s0Node = getOrCreateSubjectNode(subject0);
        	if (subject0.getChildren() != null) {
	        	for (Subject subject1 : subject0.getChildren()) {   // top level
	        		Long s1Node = getOrCreateSubjectNode(subject1, s0Node, null);
	        		if (subject0.getChildren() != null) {
		        		for (Subject subject2 : subject1.getChildren()) {    // 2nd level
		        			Long s2Node = getOrCreateSubjectNode(subject2, s1Node, subject1);
		        			inserter.createRelationship(artworkNode, s2Node, FEATURES, null);
			        	}	
	        		}
	        	}
        	}
        }
	}
	
	/**
	 * Connect artwork to a catalogue group
	 * @param artworkNode
	 * @param catalogueGroup
	 */
	private void connectArtworkToCatalogueGroup(Long artworkNode, CatalogueGroup catalogueGroup) {
		Long cgNode = getOrCreateCatalogueNode(catalogueGroup);
		inserter.createRelationship(artworkNode, cgNode, BELONGS_TO, null);
	}

	/**
	 * Get or create catalogue group node. If the node doesn't exist, create it.
	 * 
	 * @param catalogueGroup
	 * @return Physical node id
	 */
	private Long getOrCreateCatalogueNode(CatalogueGroup catalogueGroup) {
		Long cgNode = null;
		String value = lookupRepository.get(this.CATALOGUE_GROUP_KEY + catalogueGroup.getId());
		if (value == null) {
			HashMap<String, Object> properties = new HashMap<>();
	        properties.put("shortTitle", catalogueGroup.getShortTitle() != null ? catalogueGroup.getShortTitle() : "[no short title]");
	        properties.put("id", catalogueGroup.getId());
	        cgNode = inserter.createNode(properties, CATALOGUE_GROUP);
	        
	        // store new node id in lookup repository
	        lookupRepository.add(this.CATALOGUE_GROUP_KEY + catalogueGroup.getId(), Long.toString(cgNode));
		} else {
			cgNode = Long.parseLong(value);
		}
		return cgNode;
	}

	/**
	 * Connect artwork to its classification.
	 * 
	 * @param artworkNode
	 * @param classification
	 */
	private void connectArtworkToClassification(Long artworkNode,
			String classification) {
		Long clNode = getOrCreateClassification(classification);
		inserter.createRelationship(artworkNode, clNode, CLASSIFIED_AS, null);
	}

	/**
	 * Get or create classification.
	 * 
	 * @param classification
	 * @return
	 */
	private Long getOrCreateClassification(String classification) {
		Long clNode = null;
		String value = lookupRepository.get(this.CLASSIFICATION_KEY + classification);
		if (value == null) {
			HashMap<String, Object> properties = new HashMap<>();
	        properties.put("name", classification);
	        clNode = inserter.createNode(properties, CLASSIFICATION);
	        
	        // store new node id in lookup repository
	        lookupRepository.add(this.CLASSIFICATION_KEY + classification, Long.toString(clNode));
		} else {
			clNode = Long.parseLong(value);
		}
		return clNode;
	}

	/**
	 * Parse the medium string to strip out materials used.
	 * 
	 * @param artworkNode
	 * @param medium
	 */
	private void connectArtworkToMediums(Long artworkNode, String medium) {
		String[] mediums = medium.split(",| on | and ");
		
		Long mNode = null;
		for (String m : mediums) {
			String trimmed = m.trim().toLowerCase();
			String value = lookupRepository.get(this.MEDIUM_KEY + trimmed);
			if (value == null) {
				HashMap<String, Object> properties = new HashMap<>();
	        	properties.put("name", trimmed);
	        	mNode = inserter.createNode(properties, MEDIUM);
	        	lookupRepository.add(this.MEDIUM_KEY + trimmed, Long.toString(mNode));
			} else {
				mNode = Long.parseLong(value);
			}
			inserter.createRelationship(artworkNode, mNode, COMPRISED_OF, null);
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
			Long artistNode = addArtistNode(artist);
	        connectArtistToMovements(artistNode, artist.getMovements());
	        connectArtistToBirthPlace(artistNode, artist.getBirth());
		}

		// process artworks
		files = FileFinder.getFileList(artworksDirectory, "*.json");		
		for (Path f : files) {
			Artwork artwork = new ObjectMapper().readValue(f.toFile(), Artwork.class);
			try {
				Long artworkNode = addArtworkNode(artwork);			
			
				connectArtworkToArtists(artworkNode, artwork.getContributors());
				
				if (artwork.getCatalogueGroup() != null && artwork.getCatalogueGroup().getId() != null) {
					connectArtworkToCatalogueGroup(artworkNode, artwork.getCatalogueGroup());
				}
				
		        connectArtworkToMovements(artworkNode, artwork.getMovements());
		        	        
		        // connect subjects with the artwork
		        if (artwork.getSubjects() != null && artwork.getSubjects().getChildren() != null) {
		        	connectArtworkToSubjects(artworkNode, artwork.getSubjects().getChildren());
		        }
		        
		        // connect classification to the artwork
		        if (artwork.getClassification() != null) {
		        	connectArtworkToClassification(artworkNode, artwork.getClassification());
		        }
		        
		        // connect mediums to the artwork
		        if (artwork.getMedium() != null) {
		        	connectArtworkToMediums(artworkNode, artwork.getMedium());
		        }
		        
	        } catch (Exception e) {
    			System.out.println("Problem with artwork: " + artwork.getAcno());
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
