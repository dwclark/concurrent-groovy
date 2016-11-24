@Grab('org.codehaus.groovy.modules:http-builder-ng:0.10.2')
import groovy.json.JsonBuilder;
import groovyx.net.http.*;
import static Pools.*;

final File directory = new File('/home/david/tmp/counts');

def hosts = [ 'https://www.google.com', 'https://www.yahoo.com/', 'https://slashdot.org',
              'http://www.cnn.com', 'https://www.youtube.com/', 'https://www.amazon.com/',
              'https://g3summit.com/conference/fort_lauderdale/2016/11/home',
              'http://www.ford.com', 'https://www.facebook.com/' ].asImmutable()

def futures = hosts.collect { theUri ->
    HttpBuilder.configure {
        execution.maxThreads = IO_MAX;
        execution.executor = IO_POOL;
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
        return [ uriAndText[0], counts ]; }, IO_POOL)
    .thenApplyAsync({ List uriAndCounts ->
        new File(directory, uriAndCounts[0].host).text = new JsonBuilder(uriAndCounts[1]).toPrettyString();
        return uriAndCounts; }, IO_POOL); };

def mostLetters = futures.max { a, b -> a.get()[1].total <=> b.get()[1].total; }.get()
println("Site with most letters is: ${mostLetters[0]}")

shutdown();
    
