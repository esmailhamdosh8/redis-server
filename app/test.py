empty_body = bytes.fromhex("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")

a = b"+FULLRESYNC " + b"8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"+ b" 0\r\n" + f"${len(empty_body)}\r\n".encode() + empty_body

print(len(b"+PONG\r\n"))

  GNU nano 8.3                                                         django.conf                                                          Modified  
# Map the URL path /static/ to the physical directory on disk
Alias /static/ /home/ec2-user/corey/django_project/static/

<Directory /home/ec2-user/corey/django_project/static>
    Require all granted
</Directory>

Alias /media/ /home/ec2-user/corey/django_project/media/

<Directory /home/ec2-user/corey/django_project/media>
    Require all granted
</Directory>


<Directory /home/ec2-user/corey/django_project/django_project>
    <Files wsgi.py>
        Require all granted
    </Files>
</Directory>


WSGIScriptAlias / /home/ec2-user/corey/django_project/django_projects/wsgi.py
WSGIDaemonProcess django_app python-path=/home/ec2-user/corey/django_project python-home=/home/ec2-user/corey/.env
WSGIProcessGroup django_app
