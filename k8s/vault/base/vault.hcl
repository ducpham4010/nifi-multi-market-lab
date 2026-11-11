# Persistent single-node Vault (file storage), same model as the Compose lab.
# Initialization/unseal is an operator step: k8s/scripts/vault-init.py.
ui            = true
disable_mlock = true

storage "file" {
  path = "/vault/file"
}

listener "tcp" {
  address     = "0.0.0.0:8200"
  tls_disable = 1
}
