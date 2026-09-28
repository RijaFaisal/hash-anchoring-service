package com.hashanchor.persistence;

import com.hashanchor.domain.OutboxEvent;
import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    // Spring Data derives this query from the method name — no SQL or JPQL
    // to write. Oldest-first so the relay publishes in submission order.
    List<OutboxEvent> findByPublishedFalseOrderByCreatedAtAsc();
}
