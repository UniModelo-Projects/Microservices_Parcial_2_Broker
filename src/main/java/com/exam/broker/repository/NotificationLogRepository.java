package com.exam.broker.repository;

import com.exam.broker.model.NotificationLog;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.Optional;
import java.util.UUID;

public interface NotificationLogRepository extends JpaRepository<NotificationLog, UUID> {
    Optional<NotificationLog> findByOrdenIdAndTipoEvento(String ordenId, String tipoEvento);
}
