@Grab('org.codehaus.groovy.modules:http-builder-ng:0.10.2')
import groovy.json.JsonBuilder;
import groovyx.net.http.*;
import static Pools.*;

def hosts = [ 'https://www.google.com', 'https://www.yahoo.com/', 'https://slashdot.org',
              'http://www.cnn.com', 'https://www.youtube.com/', 'https://www.bing.com/',
              'https://g3summit.com/conference/fort_lauderdale/2016/11/home',
              'http://www.ford.com', 'https://www.facebook.com/' ].asImmutable();

/*
  This demonstrates the "and then and then and then and then..." pattern, but
  every task executes asynchronously. There is never any blocking here because
  of our use of completable future.

  Task:
  1) Download the main index file from the websites as text
  2) Count how many letters are on the main index page
  3) Save the results to a file

  This particular set of tasks is pretty stupid, but this is the basic
  framework for doing web crawling: pull down a file, analyze it, store the results.
  A real web crawler would probably have a queue that the analysis phase would then
  schedule work on, based on the analysis done. Extending our toy project to do this
  would simply be a matter of adding a blocking queue and looping on work to be available
  in the queue.
 */
def futures = hosts.collect { theUri ->
    HttpBuilder.configure {
        execution.maxThreads = IO_MAX;
        execution.executor = JAVA_IO_POOL;
        response.parser('text/html', NativeHandlers.Parsers.&textToString); }
    .getAsync {
        request.uri = theUri;
        URI forLater = request.uri.toURI();
        response.success { FromServer fs, String text -> [ forLater, text ]; }; }
    .thenApplyAsync({ List uriAndText ->
        Map counts = (('a'..'z') + ('A'..'Z')).inject([:]) { map, c -> map[c] = 0; map; };
        String text = uriAndText[1];
        text.each { c -> if(counts.containsKey(c)) counts[c] = counts[c] + 1; };
        counts['total'] = counts.values().sum();
        return [ uriAndText[0], counts ]; }, JAVA_IO_POOL)
    .thenApplyAsync({ List uriAndCounts ->
        new File(COUNTS_DIRECTORY, uriAndCounts[0].host).text = new JsonBuilder(uriAndCounts[1]).toPrettyString();
        return uriAndCounts; }, JAVA_IO_POOL); };

def mostLetters = futures.max { a, b -> a.get()[1].total <=> b.get()[1].total; }.get()
println("Site with most letters is: ${mostLetters[0]}")

shutdown();
    
