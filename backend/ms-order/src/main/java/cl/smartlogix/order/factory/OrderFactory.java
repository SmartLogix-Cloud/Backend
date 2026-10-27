package cl.smartlogix.order.factory;

import cl.smartlogix.order.dto.CreateOrderRequest;
import cl.smartlogix.order.model.Order;
import cl.smartlogix.order.model.OrderHistory;
import cl.smartlogix.order.model.OrderItem;
import cl.smartlogix.order.model.OrderStatus;
import cl.smartlogix.order.model.OrderType;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Patron Factory Method para la creacion de pedidos.
 *
 * <p>Define el esqueleto de construccion del agregado {@link Order} y delega en
 * cada subclase la instanciacion del pedido base propio de su tipo (el "factory
 * method" {@link #instantiate}). El armado de items, recargos e impuestos es
 * comun para no duplicar reglas entre tipos; cada tipo solo redefine lo que le
 * es propio (p. ej. el recargo via {@link #surcharge}).
 */
public abstract class OrderFactory {

    protected static final BigDecimal VAT = new BigDecimal("0.19");

    /** Tipo de pedido que este creador sabe construir. */
    public abstract OrderType type();

    /** Factory Method: cada subtipo instancia el pedido base con su configuracion. */
    protected abstract Order instantiate(CreateOrderRequest req, String code);

    /** Recargo propio del tipo sobre el subtotal (hook; por defecto, sin recargo). */
    protected BigDecimal surcharge(BigDecimal subtotal) {
        return BigDecimal.ZERO;
    }

    /** Arma el agregado completo: items, montos, impuestos e historial inicial. */
    public final Order build(CreateOrderRequest req, String code) {
        Order order = instantiate(req, code);

        BigDecimal subtotal = BigDecimal.ZERO;
        for (CreateOrderRequest.ItemRequest it : req.items()) {
            BigDecimal sub = it.unitPrice().multiply(BigDecimal.valueOf(it.quantity()));
            order.addItem(OrderItem.builder()
                .productId(it.productId())
                .sku(it.sku())
                .quantity(it.quantity())
                .unitPrice(it.unitPrice())
                .subtotal(sub)
                .build());
            subtotal = subtotal.add(sub);
        }

        BigDecimal extra = surcharge(subtotal).setScale(2, RoundingMode.HALF_UP);
        BigDecimal taxable = subtotal.add(extra);
        BigDecimal tax = taxable.multiply(VAT).setScale(2, RoundingMode.HALF_UP);

        order.setSubtotal(subtotal.setScale(2, RoundingMode.HALF_UP));
        order.setTax(tax);
        order.setTotal(taxable.add(tax).setScale(2, RoundingMode.HALF_UP));

        order.addHistory(OrderHistory.builder()
            .previousStatus(null)
            .newStatus(OrderStatus.PENDIENTE_PREPARACION)
            .reason("Pedido creado (" + type() + ")")
            .build());

        return order;
    }
}
