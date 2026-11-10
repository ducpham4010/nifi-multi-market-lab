ui = true
disable_mlock = true
storage "file" { path = "/vault/file" }
listener "tcp" { address = "0.0.0.0:18200"
 tls_disable = 1 }
