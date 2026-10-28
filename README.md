# service-library

- Contener toda la lógica de https://github.com/bee-rpc-protocol/celaut-service-libraries pero reescrita en Rust y  usando directamente https://github.com/bee-rpc-protocol/bee-rpc-over-grpc-rs para la comunicación con el Nodo del servicio.
- Contener SKILL.md para aquellos servicios que ejercen como agentes de IA: explicar todos los conceptos que le importan a un servicio (gestion de dependencias, validación del nodo, control de costes, comprobación de recibos y balances, gestión de recursos, ejecución de dependencias, gestión de redes, etc ...) y se debe estrictamente de omitir aquellos conceptos que a un servicio no le incumben (balanceo de carga entre nodos).
- Debe de indicar como desarrollar y empaquetar otros servicios o a si mismo (esto es, servicio madre). En caso de poder retornar por interfaz o instanciar un servicio originado de si mismo (tomar su codigo fuente original, modificarlo  y reempaquetar) se está consiguiendo un patron evolutivo interesante.
