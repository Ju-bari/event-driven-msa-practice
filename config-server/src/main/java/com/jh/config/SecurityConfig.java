package com.jh.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
public class SecurityConfig {

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        return http
                .securityMatcher("/**")
                .authorizeHttpRequests((authorize) -> authorize
                        .requestMatchers("/encrypt/**", "/decrypt/**", "/actuator/**", "/error").permitAll()
                        .anyRequest().authenticated()
//                        .anyRequest().permitAll() // 임시
                )
                .httpBasic(Customizer.withDefaults())
                .csrf((csrf) -> csrf.disable())
                .build();
    }

}
