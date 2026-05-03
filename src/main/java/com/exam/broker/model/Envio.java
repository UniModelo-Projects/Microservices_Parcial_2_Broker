package com.exam.broker.model;

import jakarta.persistence.*;
import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "envios")
public class Envio {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private UUID id;

    @Column(nullable = false)
    private String ordenId;

    @Column(nullable = false)
    private String clienteEmail;

    @Column(nullable = false)
    private String estado; // PENDIENTE_ENVIO, PROCESADO

    private LocalDateTime fechaCreacion;
    private LocalDateTime fechaProcesado;

    public Envio() {
        this.fechaCreacion = LocalDateTime.now();
        this.estado = "PENDIENTE_ENVIO";
    }

    // Getters y Setters
    public UUID getId() { return id; }
    public void setId(UUID id) { this.id = id; }
    public String getOrdenId() { return ordenId; }
    public void setOrdenId(String ordenId) { this.ordenId = ordenId; }
    public String getClienteEmail() { return clienteEmail; }
    public void setClienteEmail(String clienteEmail) { this.clienteEmail = clienteEmail; }
    public String getEstado() { return estado; }
    public void setEstado(String estado) { this.estado = estado; }
    public LocalDateTime getFechaCreacion() { return fechaCreacion; }
    public void setFechaCreacion(LocalDateTime fechaCreacion) { this.fechaCreacion = fechaCreacion; }
    public LocalDateTime getFechaProcesado() { return fechaProcesado; }
    public void setFechaProcesado(LocalDateTime fechaProcesado) { this.fechaProcesado = fechaProcesado; }
}
