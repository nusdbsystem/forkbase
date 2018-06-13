package configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import service.IndexService;
import service.IndexServiceImpl;

@Configuration
public class LuceneConfiguration {
	@Bean
	public IndexService indexService() {
		return new IndexServiceImpl();
	}
}
