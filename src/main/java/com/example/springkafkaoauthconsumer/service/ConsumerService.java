package com.example.springkafkaoauthconsumer.service;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.strimzi.kafka.oauth.client.ClientConfig;
import io.strimzi.kafka.oauth.common.Config;
import io.strimzi.kafka.oauth.common.ConfigProperties;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.Properties;
import java.util.concurrent.ExecutionException;


@Service
public class ConsumerService {
  private static String bootstrapServers = null;
  private static String topic = null;
  private static String clientId = null;
  private static String clientSecret = null;
  private static String consumerGroupId = null;
  private static String trustStoreLocation = null;
  private static String trustStoreType = null;
  private static String trustStorePassword = null;

  public ConsumerService(){}

  public void consume(){

    try (final KubernetesClient client = new KubernetesClientBuilder().build()){
      // topic, client_id, bootstrap
      ConfigMap configMap = client.configMaps().inNamespace("default").withName("consumer-configmap").get();
      topic = configMap.getData().get("consumer-topic");
      clientId = configMap.getData().get("consumer-client-id");
      bootstrapServers = configMap.getData().get("consumer-bootstrap-servers");
      consumerGroupId = configMap.getData().get("consumer-group-id");
      // client_secret
      Secret secret = client.secrets().inNamespace("default").withName("consumer-secret").get();
      String clientSecretEncoded = secret.getData().get("consumer-client-secret");

      clientSecret = new String(Base64.getDecoder().decode(clientSecretEncoded));
    } catch (KubernetesClientException kce){
      // log exception
    }
    System.out.println("[client_id]: " + clientId);
    System.out.println("[client_secret]: " + clientSecret);
    Properties defaults = new Properties();
    Config external = new Config();

    //  Set KEYCLOAK_HOST to connect to Keycloak host other than 'keycloak'
    //  Use 'keycloak.host' system property or KEYCLOAK_HOST env variable

    final String keycloakHost = external.getValue("keycloak.host", "localhost");
    final String realm = external.getValue("realm", "kafka");
    final String tokenEndpointUri = "http://" + keycloakHost + ":8080/realms/" + realm + "/protocol/openid-connect/token";

    //  You can also configure token endpoint uri directly via 'oauth.token.endpoint.uri' system property,
    //  or OAUTH_TOKEN_ENDPOINT_URI env variable

    defaults.setProperty(ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, tokenEndpointUri);

    //  By defaut this client uses preconfigured clientId and secret to authenticate.
    //  You can set OAUTH_ACCESS_TOKEN or OAUTH_REFRESH_TOKEN to override default authentication.
    //
    //  If access token is configured, it is passed directly to Kafka broker
    //  If refresh token is configured, it is used in conjunction with clientId and secret
    //
    //  See examples README.md for more info.

    final String accessToken = external.getValue(ClientConfig.OAUTH_ACCESS_TOKEN, null);
    System.out.println("[access_token]: " + accessToken);

    if (accessToken == null) {
      defaults.setProperty(Config.OAUTH_CLIENT_ID, clientId);
      System.out.println("[client_id]: " + clientId);

      defaults.setProperty(Config.OAUTH_CLIENT_SECRET, clientSecret);
      System.out.println("[client_secret]: " + clientSecret);
    }

    // Use 'preferred_username' rather than 'sub' for principal name
    if (isAccessTokenJwt(external)) {
      defaults.setProperty(Config.OAUTH_USERNAME_CLAIM, "preferred_username");
    }

    // Resolve external configurations falling back to provided defaults
    ConfigProperties.resolveAndExportToSystemProperties(defaults);


    //
    // common
    //

    Properties props = buildConsumerConfig();
    Consumer<String, String> consumer = new KafkaConsumer<>(props);

    for (int i = 0; ; i++) {
      try {
        consumer.subscribe(Arrays.asList(topic));

        while (true) {
          ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
          for (ConsumerRecord<String, String> record : records) {
            System.out.println("Consumed message - " + i + ": " + record.value());
          }
        }
      } catch (InterruptException e) {
        throw new RuntimeException("Interrupted while consuming message - " + i + "!");

      } catch (AuthenticationException | AuthorizationException e) {
        consumer.close();
        consumer = new KafkaConsumer<>(props);
      }

    } // end for loop
  } // end method

  @SuppressWarnings("deprecation")
  private static boolean isAccessTokenJwt(Config config) {
    String legacy = config.getValue(Config.OAUTH_TOKENS_NOT_JWT);
    if (legacy != null) {
      System.out.println("[WARN] Config option 'oauth.tokens.not.jwt' is deprecated. Use 'oauth.access.token.is.jwt' (with reverse meaning) instead.");
    }
    return legacy != null ? !Config.isTrue(legacy) :
            config.getValueAsBoolean(Config.OAUTH_ACCESS_TOKEN_IS_JWT, true);
  }

  /**
   * Build KafkaConsumer properties. The specified values are defaults that can be overridden
   * through runtime system properties or env variables.
   *
   * @return Configuration properties
   */
  private static Properties buildConsumerConfig() {

    Properties p = new Properties();

    p.setProperty("security.protocol", "SASL_PLAINTEXT");
    p.setProperty("sasl.mechanism", "OAUTHBEARER");
    p.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule sufficient ;" );
//            "login.id=" +
//            "debug=\"true\" " +
//            "OAUTH_LOGIN_SERVER=\"localhost:8080\" " +
//            "OAUTH_LOGIN_ENDPOINT='/realms/kafka/protocol/openid-connect/token' " +
//            "OAUTH_LOGIN_GRANT_TYPE=client_credentials " +
//            "OAUTH_LOGIN_SCOPE=openid " +
//            "OAUTH_AUTHORIZATION='Basic test-producer-client:4fvZCpeXAM6dTT14W8hGfuviNM8u5Kud' " +
//            "OAUTH_INTROSPECT_SERVER=localhost:8080 " +
//            "OAUTH_INTROSPECT_ENDPOINT='/realms/kafka/protocol/openid-connect/token/introspect' " +
//            "OAUTH_INTROSPECT_AUTHORIZATION='Basic test-producer-client:4fvZCpeXAM6dTT14W8hGfuviNM8u5Kud';");
    p.setProperty("sasl.login.callback.handler.class", "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler");

    p.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    p.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    p.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    p.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
    //p.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
    p.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    //p.setProperty(Config.OAUTH_SCOPE, "client_credentials");

    // TrustStore
    trustStoreLocation = "";
    p.setProperty(Config.OAUTH_SSL_TRUSTSTORE_LOCATION, trustStoreLocation);
    trustStoreType = "";
    p.setProperty(Config.OAUTH_SSL_TRUSTSTORE_TYPE, trustStoreType);
    trustStorePassword = "";
    p.setProperty(Config.OAUTH_SSL_TRUSTSTORE_PASSWORD, trustStorePassword);


    // Adjust re-authentication options
    // See: strimzi-kafka-oauth/README.md
    p.setProperty("sasl.login.refresh.buffer.seconds", "30");
    p.setProperty("sasl.login.refresh.min.period.seconds", "30");
    p.setProperty("sasl.login.refresh.window.factor", "0.8");
    p.setProperty("sasl.login.refresh.window.jitter", "0.01");

    return ConfigProperties.resolve(p);
  }
}

