package cl.smartlogix.order.factory;

import cl.smartlogix.order.dto.CreateOrderRequest;
import cl.smartlogix.order.model.Order;
import cl.smartlogix.order.model.OrderStatus;
import cl.smartlogix.order.model.OrderType;
import org.springframework.stereotype.Component;

/** Creador concreto de pedidos estandar (sin recargo). */
@Component
public class StandardOrderFactory extends OrderFactory {

    @Override
    public OrderType type() {
        return OrderType.ESTANDAR;
    }

    @Override
    protected Order instantiate(CreateOrderRequest req, String code) {
        return Order.builder()
            .code(code)
            .type(OrderType.ESTANDAR)
            .status(OrderStatus.PENDIENTE_PREPARACION)
            .customerId(req.customerId())
            .marketplaceId(req.marketplaceId())
            .build();
    }
}
