ansible-playbook create_compute_instance.yaml
ssh -i ~/.ssh/id_rsa luongphambao@35.193.43.106
ansible-playbook -i ../inventory deploy.yml