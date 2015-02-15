package io.larkin.tatesocial.utility;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.larkin.tatesocial.entity.Artwork;
import io.larkin.tatesocial.entity.Gallery;
import io.larkin.tatesocial.entity.User;
import io.larkin.tatesocial.repository.ArtworkRepository;
import io.larkin.tatesocial.repository.GalleryRepository;
import io.larkin.tatesocial.repository.UserRepository;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.neo4j.config.EnableNeo4jRepositories;
import org.springframework.data.neo4j.config.Neo4jConfiguration;
import org.springframework.data.neo4j.rest.SpringRestGraphDatabase;
import org.springframework.data.neo4j.support.Neo4jTemplate;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import com.google.common.collect.Sets;

@Configuration
@ComponentScan
@EnableAutoConfiguration
@EnableTransactionManagement
@EnableNeo4jRepositories(basePackages = "io.larkin.tatesocial.repository")
public class Neo4jUtilityApplication extends Neo4jConfiguration implements CommandLineRunner {

    public Neo4jUtilityApplication() {
    	setBasePackage("io.larkin.tatesocial");
    }
//    
//    @Bean(destroyMethod = "shutdown")
//    GraphDatabaseService graphDatabaseService() {
//    	return new GraphDatabaseFactory().newEmbeddedDatabase("/home/larkin/neo4j/data/import.graph");
//    }
//    
	@Bean(destroyMethod = "shutdown")
	SpringRestGraphDatabase graphDatabaseService() {
		return new SpringRestGraphDatabase("http://localhost:7474/db/data");
	}
	
	@Autowired
	Neo4jTemplate template;
	
	@Autowired
	ArtworkRepository artRepo;
	
	@Autowired
	UserRepository userRepo;
	
	@Autowired
	GalleryRepository galleryRepo;
	
	public void createNewGalleryForNewUser(String gName, String login, String uName, String artPattern) {
		Gallery g = new Gallery(gName);
		
		Set<Artwork> artworks = Sets.newHashSet(artRepo.findByTitle(artPattern));
		
		g.setArtworks(artworks);
		
		galleryRepo.save(g);
		
		User u = new User();
		u.setLogin(login);
		u.setName(uName);
		u.setPassword("password");
		
		Set<Gallery> galleries = new HashSet<Gallery>();
		galleries.add(g);
		u.setGalleries(galleries);
		
		userRepo.save(u);
	}
	
	
 	@Override
	public void run(String... arg0) throws Exception {
 		createNewGalleryForNewUser("Cat Album", "hkiln", "Harry Killen", "Cat");
	}
 	
 	public static void main(String[] args) {
        SpringApplication.run(Neo4jUtilityApplication.class, args);
    }


}
