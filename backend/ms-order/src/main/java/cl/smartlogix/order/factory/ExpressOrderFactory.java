package cl.smartlogix.order.factory;

import cl.smartlogix.order.dto.CreateOrderRequest;
import cl.smartlogix.order.model.Order;
import cl.smartlogix.order.model.OrderStatus;
import cl.smartlogix.order.model.OrderType;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/** Creador concreto de pedidos express: aplica un recargo por prioridad de despacho. */
@Component
public class ExpressOrderFactory extends OrderFactory {

    /** Recargo del 5% por gestion y prioridad de despacho express. */
    private static final BigDecimal EXPRESS_RATE = new BigDecimal("0.05");

    @Override
    public OrderType type() {
        return OrderType.EXPRESS;
    }

    @Override
    protected Order instantiate(CreateOrderRequest req, String code) {
        return Order.builder()
            .code(code)
            .type(OrderType.EXPRESS)
            .status(OrderStatus.PENDIENTE_PREPARACION)
            .customerId(req.customerId())
            .marketplaceId(req.marketplaceId())
            .build();
    }

    @Override
    protected BigDecimal surcharge(BigDecimal subtotal) {
        return subtotal.multiply(EXPRESS_RATE);
    }
}
