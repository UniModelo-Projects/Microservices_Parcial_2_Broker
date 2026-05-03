package com.exam.broker.model;

import jakarta.persistence.*;
import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "notification_log")
public class NotificationLog {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private UUID id;

    @Column(nullable = false)
    private String ordenId;

    @Column(nullable = false)
    private String tipoEvento; // PAGO_RECIBIDO, ENVIO_CONFIRMADO

    private LocalDateTime fechaNotificacion;

    public NotificationLog() {
        this.fechaNotificacion = LocalDateTime.now();
    }

    public NotificationLog(String ordenId, String tipoEvento) {
        this.ordenId = ordenId;
        this.tipoEvento = tipoEvento;
        this.fechaNotificacion = LocalDateTime.now();
    }

    // Getters y Setters
    public UUID getId() { return id; }
    public void setId(UUID id) { this.id = id; }
    public String getOrdenId() { return ordenId; }
    public void setOrdenId(String ordenId) { this.ordenId = ordenId; }
    public String getTipoEvento() { return tipoEvento; }
    public void setTipoEvento(String tipoEvento) { this.tipoEvento = tipoEvento; }
    public LocalDateTime getFechaNotificacion() { return fechaNotificacion; }
    public void setFechaNotificacion(LocalDateTime fechaNotificacion) { this.fechaNotificacion = fechaNotificacion; }
}
