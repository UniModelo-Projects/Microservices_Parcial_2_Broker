package com.exam.broker.repository;

import com.exam.broker.model.Envio;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import java.util.UUID;

public interface EnvioRepository extends JpaRepository<Envio, UUID> {
    List<Envio> findByEstado(String estado);
}
