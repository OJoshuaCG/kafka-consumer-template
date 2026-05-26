"""Servicios opcionales — desactivados por default.

Acá viven integraciones que NO todos los consumers necesitan:
- tenant_service: multi-tenancy
- kms_service: gestión de claves
- crypto_service: cifrado Fernet
- notification_service: webhooks / push

Mantener este módulo vacío hasta que un consumer real lo necesite.
"""
