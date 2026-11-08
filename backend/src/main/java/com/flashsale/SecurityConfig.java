package com.flashsale;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
public class SecurityConfig {
    /**
     * 创建客户端认证提供器，按配置校验客户端凭据并分配角色。
     *
     * @param properties 包含客户端 ID、密钥和角色的业务配置
     * @return 处理用户名密码认证的客户端认证提供器
     */
    @Bean
    AuthenticationProvider clients(SaleProperties properties) {
        return new AuthenticationProvider() {
            /**
             * 校验客户端 ID 和密钥，认证成功后授予 CLIENT 及配置的 ADMIN、OPERATOR 角色。
             *
             * @param authentication 待校验的认证信息，用户名为客户端 ID，密码为客户端密钥
             * @return 已认证且包含客户端 ID 和角色的令牌，不保留密钥
             * @throws BadCredentialsException 客户端不存在或密钥不匹配时抛出
             */
            @Override
            public Authentication authenticate(Authentication authentication) {
                var client = properties.clients().stream().filter(item -> item.id().equals(authentication.getName()))
                        .findFirst().orElseThrow(() -> new BadCredentialsException("Invalid client"));
                byte[] supplied = authentication.getCredentials().toString().getBytes(StandardCharsets.UTF_8);
                if (!MessageDigest.isEqual(client.secret().getBytes(StandardCharsets.UTF_8), supplied)) {
                    throw new BadCredentialsException("Invalid client");
                }
                var roles = new ArrayList<SimpleGrantedAuthority>();
                roles.add(new SimpleGrantedAuthority("ROLE_CLIENT"));
                if (client.admin()) { roles.add(new SimpleGrantedAuthority("ROLE_ADMIN")); }
                if (client.operator()) { roles.add(new SimpleGrantedAuthority("ROLE_OPERATOR")); }
                return UsernamePasswordAuthenticationToken.authenticated(client.id(), null, roles);
            }

            /**
             * 判断该提供器是否支持指定的认证类型。
             *
             * @param type 待处理的认证类型
             * @return 类型为用户名密码认证令牌或其子类时返回 {@code true}
             */
            @Override
            public boolean supports(Class<?> type) { return UsernamePasswordAuthenticationToken.class.isAssignableFrom(type); }
        };
    }

    /**
     * 配置无状态 HTTP Basic 认证，关闭 CSRF 并设置健康检查、演示资源和各接口的访问权限。
     * 未匹配访问规则的请求一律拒绝。
     *
     * @param http Spring Security 提供的 HTTP 安全配置构建器
     * @param properties 业务配置，用于判断是否放行演示页面和演示接口
     * @return 应用上述认证与授权规则的安全过滤器链
     * @throws Exception 配置或构建安全过滤器链失败时抛出
     */
    @Bean
    SecurityFilterChain security(HttpSecurity http, SaleProperties properties) throws Exception {
        http.csrf(csrf -> csrf.disable())
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(auth -> {
                    auth.requestMatchers("/actuator/health").permitAll();
                    if (properties.demoEnabled()) {
                        auth.requestMatchers("/", "/index.html", "/assets/**", "/favicon.svg", "/demo/**").permitAll();
                    }
                    auth.requestMatchers(HttpMethod.POST, "/api/v1/activities").hasRole("ADMIN")
                            .requestMatchers("/actuator/**").hasRole("OPERATOR")
                            .requestMatchers("/api/v1/events/**").hasRole("ADMIN")
                            .requestMatchers("/openapi.yaml").hasRole("CLIENT")
                            .requestMatchers("/api/**").hasRole("CLIENT")
                            .anyRequest().denyAll();
                })
                .httpBasic(Customizer.withDefaults());
        return http.build();
    }
}
