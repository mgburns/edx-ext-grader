from setuptools import setup

setup(name='edx-ext-grader',
      version='0.1',
      description='An external "pull" grader for use with the edX platform',
      url='http://github.com/mgburns/edx-ext-grader',
      author='Mike Burns',
      author_email='mgburns@bu.edu',
      packages=['edx_ext_grader'],
      install_requires=['pika', 'requests'],
      )
