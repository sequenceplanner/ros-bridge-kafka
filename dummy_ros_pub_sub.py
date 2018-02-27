#!/usr/bin/env python

#----------------------------------------------------------------------------------------
# authors, description, version
#----------------------------------------------------------------------------------------
    # Endre Eres
    # Simple dummy ROS Publisher-Subscriber mutlithreaded example: dummy_ros_pub_sub.py
    # V.1.0.0.
#----------------------------------------------------------------------------------------

import rospy
import roslib
import threading
from std_msgs.msg import String

class dummy_ros_pub_sub():

    def __init__(self):

        rospy.init_node('dummy_ros_pub_sub', anonymous=False)
        self.pub = rospy.Publisher('chatter', String, queue_size=10)
        rospy.Subscriber("chatter", String, self.topic1_callback)
        self.pub2 = rospy.Publisher('chatter2', String, queue_size=10)
        rospy.Subscriber("chatter2", String, self.topic2_callback)
        self.rate = rospy.Rate(2)
        self.rate2 = rospy.Rate(3)
        self.topic1()
        self.topic2()

    def topic1(self):
        def topic1_callback_local():
            while not rospy.is_shutdown():
                hello_str = "ROS: hello world" 
                rospy.loginfo(hello_str)
                self.pub.publish(hello_str)
                self.rate.sleep()
        t = threading.Thread(target=topic1_callback_local)
        t.daemon = True
        t.start()
    
    def topic2(self):
        def topic2_callback_local():
            while not rospy.is_shutdown():
                hello_str2 = "ROS: hello world 2"
                rospy.loginfo(hello_str2)
                self.pub2.publish(hello_str2)
                self.rate2.sleep()
        t = threading.Thread(target=topic2_callback_local)
        t.daemon = True
        t.start()

        rospy.spin()

    def topic1_callback(self, topic1_data):
        rospy.loginfo("I heard %s", topic1_data.data)

    def topic2_callback(self, topic2_data):
        rospy.loginfo("I heard %s", topic2_data.data)
  
if __name__ == '__main__':
    try:
        dummy_ros_pub_sub()
    except rospy.ROSInterruptException:
        pass