# *****************************************************************************
# Copyright (c) 2016 Adam Milton-Barker and other Contributors.
#
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Public License v1.0
# which accompanies this distribution, and is available at
# http://www.eclipse.org/legal/epl-v10.html 
#
# Contributors:
#   Adam Milton-Barker - Intel Software Innovator
# *****************************************************************************

import inspect
import json
import os
import paho.mqtt.client as mqtt
import sys
import time

class ApplicationConnection():
	
	def __init__(self, configs):
    		
		print("-- Initiating JumpWayMQTT Application")
    			
		self._configs = configs
		self.mqttClient = None
		self.mqttTLS =  os.path.dirname(os.path.abspath(__file__)) + "/ca.pem"
		self.mqttHost = 'iot.techbubbletechnologies.com'
		self.mqttPort = 8883

		self.applicationStatusCallback = None
		self.deviceStatusCallback = None	
		self.deviceSensorCallback = None
		self.deviceCommandsCallback = None
		self.deviceNotificationsCallback = None
		self.deviceNotificationsCallback = None

		if self._configs['locationID'] == None:
    			
			raise ConfigurationException("** Location ID (locationID) property is required")

		elif self._configs['applicationID'] == None:
    			
			raise ConfigurationException("** Application ID (applicationID) property is required")

		elif self._configs['applicationName'] == None:
    			
			raise ConfigurationException("** Application Name (applicationName) property is required")

		elif self._configs['username'] == None: 

			raise ConfigurationException("** MQTT UserName (username) property is required")

		elif self._configs['password'] == None: 

			raise ConfigurationException("** MQTT Password (password) property is required")
    		
		print("-- JumpWayMQTT Application Initiated")
	
	def connectToApplication(self):
    		
		print("-- JumpWayMQTT Application Connection Initiating")
    		
		self.mqttClient = mqtt.Client(client_id=self._configs['applicationName'], clean_session=True)
		applicationStatusTopic = '%s/Applications/%s/Status' % (self._configs['locationID'], self._configs['applicationID'])
		self.mqttClient.will_set(applicationStatusTopic, "OFFLINE", 0, False)
		self.mqttClient.tls_set(self.mqttTLS, certfile=None, keyfile=None)
		self.mqttClient.on_connect = self.on_connect
		self.mqttClient.on_message = self.on_message
		self.mqttClient.on_publish = self.on_publish
		self.mqttClient.on_subscribe = self.on_subscribe
		self.mqttClient.username_pw_set(str(self._configs['username']),str(self._configs['password']))
		self.mqttClient.connect(self.mqttHost,self.mqttPort,10)
		self.mqttClient.loop_start()
    		
		print("-- JumpWayMQTT Application Connection Initiated")

	def on_connect(self, client, obj, flags, rc):
        
			print("-- JumpWayMQTT Application Connected")
			print("rc: "+str(rc))
    		
			self.publishToApplicationStatus("ONLINE")

	def on_subscribe(self, client, obj, mid, granted_qos):
    		
			print("JumpWayMQTT Subscription: "+str(self._configs['applicationName']))

	def on_message(self, client, obj, msg):
    		
		print("JumpWayMQTT Message Received")
		splitTopic=msg.topic.split("/")
		
		if splitTopic[1]=='Applications':
    			
			if splitTopic[3]=='Status':
    				
				if self.applicationStatusCallback == None:
    					
					print("** Application Status Callback Required (applicationStatusCallback)")

				else:
    					
					self.applicationStatusCallback(msg.topic,msg.payload)

			elif splitTopic[3]=='Camera':
    				
				if self.cameraCallback == None:
    					
					print("** Application Camera Callback Required (cameraCallback)")

				else:
    					
					self.cameraCallback(msg.topic,msg.payload)

			elif splitTopic[3]=='Notifications':
    				
				if self.applicationNotificationsCallback == None:
    					
					print("** Application Notification Callback Required (applicationNotificationsCallback)")

				else:
    					
					print("Sent To deviceNotificationsCallback")
					self.deviceNotificationsCallback(msg.topic,msg.payload)

		elif splitTopic[1]=='Devices':
    			
			if splitTopic[4]=='Status':
    				
				if self.deviceStatusCallback == None:
    					
					print("** Device Status Callback Required (deviceStatusCallback)")

				else:
    					
					self.deviceStatusCallback(msg.topic,msg.payload)

			elif splitTopic[4]=='Sensors':
    				
				if self.deviceSensorCallback == None:
    					
					print("** Device Sensors Callback Required (deviceSensorCallback)")

				else:
					self.deviceSensorCallback(msg.topic,msg.payload)

			elif splitTopic[4]=='Actuators':
    				
				if self.deviceActuatorCallback == None:
    					
					print("** Device Actuator Callback Required (deviceActuatorCallback)")
					
				else:
    					
					self.deviceActuatorCallback(msg.topic,msg.payload)

			elif splitTopic[4]=='Commands':
    				
				if self.deviceCommandsCallback == None:
    					
					print("** Device Commands Callback Required (deviceCommandsCallback)")
					
				else:
    					
					self.deviceCommandsCallback(msg.topic,msg.payload)

			elif splitTopic[4]=='Notifications':
    				
				if self.deviceNotificationsCallback == None:
    					
					print("** Device Notifications Callback Required (deviceNotificationsCallback)")

				else:
    					
					self.deviceNotificationsCallback(msg.topic,msg.payload)

			elif splitTopic[4]=='Camera':
    				
				if self.cameraCallback == None:
    					
					print("** Device Camera Callback Required (cameraCallback)")

				else:
    					
					self.cameraCallback(msg.topic,msg.payload)
	
	def publishToApplicationStatus(self, data):
    		
		if self._configs['locationID'] == None:
			print("locationName is required!")
			return False

		elif self._configs['applicationID'] == None:
			print("applicationID is required!")
			return False
			
		else:
			deviceStatusTopic = '%s/Applications/%s/Status' % (self._configs['locationID'], self._configs['applicationID'])
			self.mqttClient.publish(deviceStatusTopic,data)
			print("Published to Application Status "+deviceStatusTopic)
			
	def publishToApplicationChannel(self, channel, application, data):
        
		if self._configs['locationID'] == None:
    			
			print("** Location ID (locationID) is required!")
			return False

		elif application == None:

			print("** Application ID (application) is required!")
			return False

		else:
    			
			applicationChannel = '%s/Applications/%s/%s' % (self._configs['locationID'], application, channel)
			self.mqttClient.publish(applicationChannel,json.dumps(data))
			print("Published to Application "+channel+" Channel")
	
	def subscribeToApplicationChannel(self, applicationID, channelID, qos=0):

		if self._configs['locationID'] == None:
    			
			print("** Location ID (locationID) is required!")
			return False

		elif applicationID == None:

			print("** Application ID (applicationID) is required!")
			return False

		elif channelID == None:
					
				print("** Channel ID (channelID) is required!")
				return False

		else:
    			
			if applicationID == "All":
    			
				applicationChannel = '%s/Applications/#' % (self._configs['locationID'])
				self.mqttClient.subscribe(applicationChannel, qos=qos)
				print("-- Subscribed to all Application Channels")
				return True
				
			else:
    			
				applicationChannel = '%s/Applications/%s/%s' % (self._configs['locationID'], applicationID, channelID)
				self.mqttClient.subscribe(applicationChannel, qos=qos)
				print("-- Subscribed to Application "+channelID+" Channel")
				return True
			
	def publishToDeviceChannel(self, channel, zone, device, data):
        
		if self._configs['locationID'] == None:
    			
			print("** Location ID (locationID) is required!")
			return False

		elif zone == None:

			print("** Zone ID (zoneID) is required!")
			return False

		elif device == None:

			print("** Device ID (device) is required!")
			return False

		else:
    			
			deviceChannel = '%s/Devices/%s/%s/%s' % (self._configs['locationID'], zone, device, channel)
			self.mqttClient.publish(deviceChannel,json.dumps(data))
			print("Published to Device "+channel+" Channel")
	
	def subscribeToDeviceChannel(self, zone, device, channel, qos=0):
    
		if self._configs['locationID'] == None:
    			
			print("** Location ID (locationID) is required!")
			return False

		elif zone == None:

			print("** Zone ID (zoneID) is required!")
			return False

		elif device == None:

			print("** Device ID (device) is required!")
			return False

		elif channelID == None:
					
				print("** Channel ID (channelID) is required!")
				return False
				
		else:
    			
			deviceChannel = '%s/Devices/%s/%s/%s' % (self._configs['locationID'], zone, device, channel)
			self.mqttClient.subscribe(deviceChannel, qos=qos)
			print("Subscribed to Device "+channel+" Channel")
			return True

	def on_publish(self, client, obj, mid):
    		
			print("-- Published: "+str(mid))

	def on_log(self, client, obj, level, string):
    		
			print(string)
	
	def disconnectFromApplication(self):
		self.publishToApplicationStatus("OFFLINE")
		self.mqttClient.disconnect()	
		self.mqttClient.loop_stop()
	
			
