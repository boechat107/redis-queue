from setuptools import setup

setup(name='redis-queue-pyclj',
      version='0.2.1',
      description='Some fault tolerance over Redis lists',
      url='https://github.com/boechat107/redis-queue',
      author='Andre A. Boechat',
      author_email='boechat107@gmail.com',
      license='Eclipse',
      py_modules=['redis_queue'],
      install_requires=[
          'redis >= 2.9.0'
      ],
      zip_safe=False)
