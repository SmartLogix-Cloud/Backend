package cl.smartlogix.order.repository;

import cl.smartlogix.order.model.OrderTracking;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface OrderTrackingRepository extends JpaRepository<OrderTracking, Long> {
}
