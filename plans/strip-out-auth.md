# Strip Out Auth
**Status:** Unapplied

Authentication was added by accident. This project will completely avoid handling or managing authentication and leave it to the deployment configuration. For example a very common setup may be to deploy on kubernetes with envoy+wayfinder doing auth in front of it. 