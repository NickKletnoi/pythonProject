/content/drive/MyDrive/test/
-------------------------------
--- for mouting----------------
from google.colab import drive
drive.mount('/content/drive')
------------------------------
--- for running pip-----------
%%bash
sudo apt-get install -y unzip
-------------------------------
---- for loading program ------ 
%%bash
sudo apt-get install -y unzip
----------------------------------
-- for installing stuff ----------
!pip install -Uqqq pip --progress-bar off
!pip install -Uqqq gdown --progress-bar off
!pip install -Uqqq torch --progress-bar off
!pip install -Uqqq watermark --progress-bar off
!pip install -Uqqq torchview --progress-bar off
-------------------------------------
--- get the versions of what is installed:

%watermark --iversions

