This will host all the grunt work/experimentals scripts regarding pricing

Things to install 
-----------------
apt install python2.7
apt install python-tk
pip install requests setuptools numpy scipy pandas Matplotlib Seaborn Bokeh sciKit-Learn TensorFlow Keras Statsmodels elasticsearch



Glitches
---------

if get memory error while installing above tools

sudo /bin/dd if=/dev/zero of=/var/swap.1 bs=1M count=1024
sudo /sbin/mkswap /var/swap.1
sudo /sbin/swapon /var/swap.1

