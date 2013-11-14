import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.contains;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;
import java.util.Properties;

import org.jclouds.ContextBuilder;
import org.jclouds.apis.ApiMetadata;
import org.jclouds.apis.Apis;
import org.jclouds.atmos.AtmosAsyncClient;
import org.jclouds.atmos.AtmosClient;
import org.jclouds.azureblob.AzureBlobAsyncClient;
import org.jclouds.azureblob.AzureBlobClient;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.domain.StorageType;
import org.jclouds.openstack.swift.SwiftAsyncClient;
import org.jclouds.openstack.swift.SwiftClient;
import org.jclouds.providers.ProviderMetadata;
import org.jclouds.providers.Providers;
import org.jclouds.rest.RestContext;
import org.jclouds.s3.S3AsyncClient;
import org.jclouds.s3.S3Client;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.inject.Module;

/**
 * Demonstrates the use of {@link BlobStore}.
 * 
 * Usage is: java MainApp \"provider\" \"identity\" \"credential\" \"containerName\"
 * 
 * @author Carlos Fernandes
 * @author Adrian Cole
 */
public class Sample {
   
   public static final Map<String, ApiMetadata> allApis = Maps.uniqueIndex(Apis.viewableAs(BlobStoreContext.class),
        Apis.idFunction());
   
   public static final Map<String, ProviderMetadata> appProviders = Maps.uniqueIndex(Providers.viewableAs(BlobStoreContext.class),
        Providers.idFunction());
   
   public static final Set<String> allKeys = ImmutableSet.copyOf(Iterables.concat(appProviders.keySet(), allApis.keySet()));
   
   public static int PARAMETERS = 4;
   public static String INVALID_SYNTAX = "Invalid number of parameters. Syntax is: \"provider\" \"identity\" \"credential\" \"containerName\" ";

   public static void main(String[] args) throws IOException {

      if (args.length < PARAMETERS)
         throw new IllegalArgumentException(INVALID_SYNTAX);

      // Args

      String provider = args[0];

      // note that you can check if a provider is present ahead of time
      checkArgument(contains(allKeys, provider), "provider %s not in supported list: %s", provider, allKeys);

      String identity = args[1];
      String credential = args[2];
      String containerName = args[3];

      Properties overrides = new Properties();
      overrides.setProperty("jclouds.endpoint", "http://localhost:8080");
      overrides.setProperty("jclouds.s3.virtual-host-buckets", "false");

      // Init
      BlobStoreContext context = ContextBuilder.newBuilder(provider)
                                               .credentials(identity, credential)
                                               .overrides(overrides)
                                               .buildView(BlobStoreContext.class);

      try {

         // Create Container
         BlobStore blobStore = context.getBlobStore();
         blobStore.createContainerInLocation(null, containerName);

         // Add Blob
         Blob blob = blobStore.blobBuilder("test").payload("testdata").build();
         blobStore.putBlob(containerName, blob);

         // List Container
         for (StorageMetadata resourceMd : blobStore.list()) {
            if (resourceMd.getType() == StorageType.CONTAINER || resourceMd.getType() == StorageType.FOLDER) {
               // Use Map API
               Map<String, InputStream> containerMap = context.createInputStreamMap(resourceMd.getName());
               System.out.printf("  %s: %s entries%n", resourceMd.getName(), containerMap.size());
            }
         }
         blobStore.clearContainer(containerName);

      } catch (Exception ex) {
          ex.printStackTrace();
          System.exit(-1);
      } finally {
         // Close connecton
         context.close();
      }
      System.exit(0);

   }
}
