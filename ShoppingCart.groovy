import groovy.transform.*;
import groovyx.gpars.agent.Agent;
import groovyx.gpars.dataflow.DataflowVariable;
import java.util.concurrent.ThreadLocalRandom;
import static Pools.*;

class DiscountService {
    final Map<Integer,BigDecimal> discounts = new TreeMap([ 2: 0.05, 5: 0.07, 10: 0.1 ]).asImmutable();
    
    void process(Agent<CartState> agent) {
        agent << { CartState s ->
            s.discount = 0.0;
            discounts.each { int count, BigDecimal discount ->
                if(s.items.size() >= count) s.discount = discount; }; };
    }
}

@ToString class Item {
    BigDecimal cost;
    int quantity;
    String description;
    Item copy() { new Item(cost: cost, quantity: quantity, description: description); }
}

@ToString class CartState {
    List<Item> items = [];
    BigDecimal discount = 0.0;
    CartState copy() { new CartState(items: items.collect { it.copy(); }, discount: discount); }
}

class Cart {
    final private Agent<CartState> theAgent;
    final private DiscountService service;

    public Cart(DiscountService service) {
        this.service = service;
        this.theAgent = new Agent(new CartState());
        theAgent.attachToThreadPool(COMPUTE_POOL);
    }

    void add(Item item) {
        theAgent << { CartState s -> s.items.add(item); }
        service.process(theAgent);
    }

    void remove(int number) {
        theAgent << { CartState s ->
            if(number < s.items.size()) {
                s.items.remove(number)
            } };
        service.process(theAgent);
    }

    DataflowVariable<CartState> getStatePromise() {
        DataflowVariable<CartState> var = new DataflowVariable<>();
        theAgent << { CartState s -> var << s.copy(); }
        return var
    }
    
    CartState getState() {
        return statePromise.val;
    }
}

initialize();
DiscountService service = new DiscountService();
Cart cart = new Cart(service);

def randomAction = { ->
    ThreadLocalRandom r = ThreadLocalRandom.current();
    if(r.nextInt(10) < 6) {
        cart.remove(0);
    }
    else {
        cart.add(new Item(cost: r.nextInt(1, 10),
                          quantity: r.nextInt(1, 10),
                          description: randomText(10)));
    }
}

def threads = (0..8).collect { 
    Thread.start {
        100.times {
            randomAction(); }; }; };

threads.each { it.join(); }
CartState finalState = cart.state;
println("Quantity: ${finalState.items.size()}, Discount: ${finalState.discount}");

shutdown();
