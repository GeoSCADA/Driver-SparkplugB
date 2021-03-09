using System;
using System.Collections.Generic;
using System.Text;
using ClearSCADA.DBObjFramework;
using ClearSCADA.DriverFramework;
using SparkplugB;
using uPLibrary.Networking.M2Mqtt;
using System.Web.Script.Serialization;
using ClearScada.Client;
using System.Security.Cryptography.X509Certificates;
using System.Linq;
using System.Diagnostics;
using Google.Protobuf;
using Org.Eclipse.Tahu.Protobuf;

namespace DriverSparkplugB
{

    class DriverSparkplugB
	{
		static void Main(string[] args)
		{
			// Debugger.Launch();

			using (DriverApp App = new DriverApp())
			{
				// Init the driver with the database module
				if (App.Init(new CSharpModule(), args))
				{
					// Do custom driver init here
					App.Log("SparkplugB driver started");

					// Start the driver MainLoop
					App.MainLoop();
				}
			}
		}
	}



    public class DrvSparkplugBBroker : DriverChannel<SparkplugBBroker>
    {
        // List of known devices
        public Dictionary<String, DrvCSScanner> DeviceIndex = new Dictionary<String, DrvCSScanner>();
        // List of unknown devices. Persisted to database module as a string of uuid and names.
        public Dictionary<string, configItem> configBuf = new Dictionary<string, configItem>();
		// List of pending data items to process, received in birth but need processing later.
		public Dictionary<string, Payload> dataBuf = new Dictionary<string, Payload>();
		// Last known online state
		//private bool LastKnownOnline = false;
		// Last known connection state
		private bool LastKnownConnect = false;
        // The MQTT client
        private MqttClient client;
		// List of potentially new devices. Needed to retain conf required state in status, until config is completed
		// Not needed at present for Sparkplug
		public class ActionFlags
		{
			public bool confReq;
		}
		public Dictionary<string, ActionFlags> actionBuf = new Dictionary<string, ActionFlags>();

		public void LogAndEvent( string Message)
		{
			Log(Message);
			if (this.DBChannel.EnhancedEvents)
			{
				App.SendReceiveObject(this.DBChannel.Id, OPCProperty.SendRecLogBrokerEventText, Message);
			}
		}

		public override void OnDefine()
        {
            // Called (first) when enabled or config changed. Not on startup
            base.OnDefine();
            LogAndEvent("Defined: " + (string)DBChannel.BrokerHost);

            // Clear the internal index of devices (scanners)
            DeviceIndex.Clear();
        }

        public override void OnConnect()
        {
            Log("Channel OnConnect(): Start, call base");
			base.OnConnect();

			// Make a broker connection
			ServerStatus ss = DoConnect();
            if (ss == ServerStatus.Offline)
            {
                LogAndEvent("Failed to connect.");
                throw new System.Runtime.InteropServices.COMException("MQTT Broker connection unsuccessful");
            }
            else
            {
                LastKnownConnect = true;
                LogAndEvent("OnConnect Channel Online.");
            }
            Log("Channel OnConnect(): End");
        }

        public override void OnPoll()
        {
            // Defaults to 30 seconds.
            Log("OnPoll");
            // No need to call base class. Return exception on failure of connection
            if (!client.IsConnected)
            {
                if (LastKnownConnect == true)
                {
                    SetStatus(ServerStatus.Offline, "Cannot connect");
                    SetFailReason("Could not open broker connection. ");
                    LastKnownConnect = false;
                    LogAndEvent("OnPoll: Cannot connect.");
                    throw new System.Runtime.InteropServices.COMException("MQTT Broker connection unsuccessful.");
                }
            }
        }

        public override void OnDisconnect()
        {
            Log("Channel OnDisConnect(): Start");
            if (client.IsConnected)
            {
                LogAndEvent("Channel OnDisConnect(): Need to disconnect MQTT");
                client.Disconnect();
            }

            Log("Channel OnDisConnect(): Call base");
            base.OnDisconnect();

            Log("Channel OnDisConnect(): End");
        }

        public override void OnUnDefine()
        {
            Log("Channel OnUndefine(): Start");

            Log("Channel OnUndefine(): Call base");
            base.OnUnDefine();

            Log("Channel OnUndefine(): End");
        }


        // Called on (re)connection.
        private ServerStatus DoConnect()
        {
            Log("Channel doConnect(): Call Conn...");
            string errorText;

			ConnectivityOptions ConnOptions = new ConnectivityOptions
			{
				hostname = (string)DBChannel.BrokerHost,
				portnumber = DBChannel.BrokerPort,
				username = DBChannel.Username,
				password = DBChannel.Password,
				version = DBChannel.MQTTVersion,
				clientId = DBChannel.ClientId,
				security = DBChannel.Security,
				caCertFile = DBChannel.caCertFile,
				clientCertFile = DBChannel.clientCertFile,
				clientCertFormat = DBChannel.ClientCertFormat,
				clientCertPassword = DBChannel.ClientCertPassword,
				CleanConnect = DBChannel.CleanConnect,
				SubQoS = DBChannel.SubQoS,
				PubQoS = DBChannel.PubQoS
			};

            bool s = ConnectTo(ConnOptions, out errorText);

            Log("Channel doConnect(): Call CheckIfBrokerIsActive...");
            // Check Broker's active status
            if (s) 
            {
                Log("Channel doConnect() set Connected");

                SetStatus(ServerStatus.Online, "Connected");

                // Alarm Clear
                //Log("Channel doConnect(): SendReceive to Clear Alarm");
                //App.SendReceiveObject(DBChannel.Id, SendRecClearChannelAlarm, true);
                //if (!LastKnownOnline)
                //{
                //    Log("Channel doConnect(): Call SetScannersOnline");
                //    //SetScannersOnline();
                //    LastKnownOnline = true;
                //}
                return ServerStatus.Online;
            }
            else
            {
                Log("Channel doConnect(): false");

                LogAndEvent("Channel doConnect(): Set Status Offline");
                SetFailReason("Could not open broker connection. " + errorText);

                SetStatus(ServerStatus.Offline, "Cannot connect");

                // Alarm Active
                //Log("Channel doConnect(): SendReceive to Activate Alarm");
                //App.SendReceiveObject(DBChannel.Id, OPCProperty.SendRecRaiseChannelAlarm, true);
                //if (LastKnownOnline)
                //{
                //    Log("Channel doConnect(): Call SetScannersOffline");
                //    //SetScannersOffline();
                //    LastKnownOnline = false;
                //}
                return ServerStatus.Offline;
            }
        }

#if false // Not needed.
        private void SetScannersOffline()
        {
            Log("SetScannersOffline(): Start");

            // set the scanners that are attached to this channel offline
            // when the channel is failed
            foreach (DrvCSScanner s in Scanners)
            {
                // Set scanner offline
                Log("SetScannersOffline(): Setting Scanner '" + s.FullName + "' Offline");

                s.SetStatus(SourceStatus.Offline);
                s.SetFailReason("Channel is offline");
                removeScannerFromIndex(s);

                // Scanner Alarm Active - TO DO
                //App.SendReceiveObject(s.DBScanner.Id, OPCProperty.SendRecRaiseScannerAlarm, true);
            }

            Log("SetScannersOffline(): End");
        }

        private void SetScannersOnline()
        {
            Log("SetScannersOnline(): Start");
            // set the scanners that are attached to this channel offline
            // when the channel is failed
            foreach (DrvCSScanner s in Scanners)
            {
                // Set scanner online
                Log("SetScannersOnline(): Setting Scanner '" + s.FullName + "' Online");

                s.SetStatus(SourceStatus.Online);

                // Scanner Alarm Active - TO DO
                //App.SendReceiveObject(DBChannel.Id, OPCProperty.SendRecClearScannerAlarm, true);
                addScannerToIndex(s);
            }
            Log("SetScannersOnline(): End");
        }
#endif

        public void AddScannerToIndex(DrvCSScanner s)
        {
            // Check if channel already contains scanner before adding
            if (DeviceIndex.ContainsKey((string)s.DBScanner.NodeDevice) == false)
            {
                DeviceIndex.Add(s.DBScanner.NodeDevice, s);
                LogAndEvent("Add to Device Dictionary: " + DeviceIndex.Count);
            }
            else
            {
				LogAndEvent("NOT Added to Device Dictionary - Duplicate " + DeviceIndex.Count + " | scanner " + s.ToString());
            }
        }

        public void RemoveScannerFromIndex(DrvCSScanner s)
        {
            if (DeviceIndex.ContainsKey((string)s.DBScanner.NodeDevice) == true)
            {
                DeviceIndex.Remove(s.DBScanner.NodeDevice);
				LogAndEvent("Removed from Device Dictionary: " + DeviceIndex.Count);
            }
            else
            {
				LogAndEvent("NOT found for deletion in Device Dictionary " + DeviceIndex.Count + " | scanner " + s.ToString());
            }
        }

        private bool ConnectTo(ConnectivityOptions c, out string errorText)
        {
            try
            {
                // create client instance
                if (c.security == 0)
                {
                    client = new MqttClient(c.hostname, c.portnumber, false, MqttSslProtocols.None, null, null);
                }
                else
                {
                    X509Certificate caCert;
                    X509Certificate clientCert;
                    try
                    {
                        caCert = new X509Certificate(c.caCertFile);
                    }
                    catch (Exception e)
                    {
                        LogAndEvent("Error reading or creating certificate from CA Certificate file: " + e.Message);
                        throw e;
                    }
                    try
                    {
						if (c.clientCertFormat == 0) // DER
						{
							clientCert = new X509Certificate(c.clientCertFile);
						}
						else if (c.clientCertFormat == 1) // PFX
						{
							clientCert = new X509Certificate2(c.clientCertFile, c.clientCertPassword);
						}
						else
                        {
							LogAndEvent("Cert format not supported");
							throw new Exception("Cert format not supported");
                        }
                    }
                    catch (Exception e)
                    {
                        LogAndEvent("Error reading or creating certificate from Client Certificate file: " + e.Message);
                        throw e;
                    }
                    client = new MqttClient(c.hostname, c.portnumber, true, caCert, clientCert, (MqttSslProtocols)c.security);
                }

                if (c.version == 0)
                {
					LogAndEvent("Protocol version 3.1");
                    client.ProtocolVersion = MqttProtocolVersion.Version_3_1;
                }
                else if (c.version == 1)
                {
					LogAndEvent("Protocol version 3.1.1");
                    client.ProtocolVersion = MqttProtocolVersion.Version_3_1_1;
                }
                else
                {
                    LogAndEvent("Unknown protocol version");
                }
            }
            catch (Exception Fault)
            {
                LogAndEvent("Exception (Create client): " + Fault.Message);
                errorText = Fault.Message;
                return false;
            }
            // register to message received 
            client.MqttMsgPublishReceived += Client_MqttMsgPublishReceived;
            client.ConnectionClosed += Client_MqttConnectionClosed;

			// Death certificate
			string StateTopic = "STATE/" + DBChannel.SCADAHostId;
			string DeathPayload = "OFFLINE";
			LogAndEvent("Death Topic: " + StateTopic);

			//string clientId = Guid.NewGuid().ToString();
			try
            {
				// Note AWS doesn't support retain, so use CleanConnect
                if (c.username == null || c.username == "")
                {
                    // create client instance 
                    client.Connect(c.clientId, "", "", !c.CleanConnect, 1, true, StateTopic, DeathPayload, c.CleanConnect, 60);
                }
                else
                {
                    // create client instance with login.
                    client.Connect(c.clientId, c.username, c.password, !c.CleanConnect, 1, true, StateTopic, DeathPayload, c.CleanConnect, 60);
                }
            }
            catch (Exception Fault)
            {
                // Cannot connect to broker
                LogAndEvent("Exception (Connect client): " + Fault.Message);
                errorText = Fault.Message;
                return false;
            }

			// Send birth certificate
			string BirthPayload = "ONLINE";
			byte[] BirthPayloadBytes = Encoding.ASCII.GetBytes( BirthPayload);
			DoPublish(StateTopic, BirthPayloadBytes, c.PubQoS, true);

			// subscribe to the topic (namespace / group) with wildcard and QoS 2 MqttMsgBase.QOS_LEVEL_EXACTLY_ONCE
			// Can we do this to the local broker to ensure safe delivery?
			string topic = this.DBChannel.NamespaceName + "/" + this.DBChannel.GroupId + "/#";
            ushort s = client.Subscribe(new string[] { topic }, new byte[] { c.SubQoS });
            LogAndEvent("Subscribed to: " + topic + " - with QoS: " + c.SubQoS.ToString());
            errorText = "";

            return true;
        }

		public bool DoPublish( string topic, byte [] message, byte qosLevel, bool retain)
		{

			if (!DBChannel.CleanConnect)
			{
				client.Publish(topic, message, qosLevel, retain);
			}
			else
            {
				client.Publish(topic, message, qosLevel, false); // For AWS that doesn't support retained flag currently
			}

			LogAndEvent("Published to: " + topic + " size: " + message.Length.ToString());
			return true;
		}

        private void Client_MqttConnectionClosed(object sender, System.EventArgs e)
        {
            this.SetFailReason("Connection Closed: " + e.ToString());
            LogAndEvent("MQTT Connection closed.");
			SetStatus(ServerStatus.Failed, e.ToString());
		}

        // 
        private void Client_MqttMsgPublishReceived(object sender, uPLibrary.Networking.M2Mqtt.Messages.MqttMsgPublishEventArgs e)
        {
            // handle message received 
            string t = e.Topic;
            LogAndEvent("RX Topic: " + t);

			// We received a byte string e.Message
            LogAndEvent("RX Message bytes: " + e.Message.Count() );

			// Find the point entry with this topic
			string Namespace = "", GroupId = "", Command = "", Node = "", Device = "";
            DecodeTopic( t, ref Namespace, ref GroupId, ref Command, ref Node, ref Device);

			// Checks
			if (Namespace != DBChannel.NamespaceName)
			{
				LogAndEvent("Wrong namespace");
				return;
			}
			if (GroupId != DBChannel.GroupId)
			{
				LogAndEvent("Wrong Group Id");
				return;
			}

            if (Node == "")
            {
                // Empty Node
                LogAndEvent("Empty Node");
				return;
            }
            else
            {
                // Check message command is valid
                switch (Command)
                {
                    case "NDEATH":
                        ProcessInLwt( Node, e.Message);
                        break;
                    case "NBIRTH":
                        ProcessInStatus( Node, e.Message);
                        break;
					case "NDATA":
						ProcessInData(Node, e.Message);
						break;
					case "DDEATH":
						ProcessInLwt(Node + "/" + Device, e.Message);
						break;
					case "DBIRTH":
						ProcessInStatus(Node + "/" + Device, e.Message);
						break;
					case "DDATA":
						ProcessInData(Node + "/" + Device, e.Message);
						break;

					default:
                        LogAndEvent("Unknown command: " + Command);
                        break;
                }
            }
        }

		// Convert topic string into separate fields
		// namespace/group_id/DDEATH/edge_node_id/device_id

		// Result[0] is namespace, Result[1] is groupid, Result[2] is command, Result[3] is Node, Result[4] is device
		private bool DecodeTopic( string Topic, ref string Namespace, ref string GroupId, ref string Command, ref string Node, ref string Device)
        {
            string[] t = Topic.Split('/');
			if (t.Length >= 4)
			{
				Namespace = t[0];
				GroupId = t[1];
				Command = t[2];
				Node = t[3];
				if (t.Length == 5)
				{
					Device = t[4];
				}
				return true;
			}
			else
			{
				// Invalid data
				return false;
			}
        }

        private void ProcessInLwt( string NodeDeviceId, byte [] data)
        {
            Log("ProcessInLwt");
			// A device has disconnected. 

			// Sparkplug documentation is a little ambiguous
			// NDEATH has a payload with a timestamp (presumably of birth time) 
			//  and a UInt64 metric 'bdSeq' - equal to Seq of the Birth?
			// DDEATH has no payload
			// But in 16.7 DDEATH must include a seq number one greater than previous?
			// And it also states "A NBIRTH message must always contain a sequence number of zero"
			// which is inconsistent with 7.1.1
			// We will process a payload if present and send any bdSeq and seq to the server

            DrvCSScanner FD;
            if (DeviceIndex.TryGetValue(NodeDeviceId, out FD))
            {
				// Interpret ProtoBuf
				Int16 seqNo = 0;
				Payload LWTPayload;
				try
				{
					LWTPayload = Payload.Parser.ParseFrom(data);

					if (LWTPayload.Metrics.Count > 0)
					{
						foreach (Payload.Types.Metric p in LWTPayload.Metrics)
						{
							if (p.Name == "bdSeq")
							{
								seqNo = (Int16)p.IntValue;
								break;
							}
						}
					}
					App.SendReceiveObject(FD.DBScanner.Id, OPCProperty.SendRecProcessLWT, seqNo);
				}
				catch (Exception e)
				{
					LogAndEvent("Error interpreting Death message for: " + NodeDeviceId + " " + e.Message);
				}

				App.SendReceiveObject(FD.DBScanner.Id, OPCProperty.SendRecRaiseScannerAlarm, data);

				FD.SetStatus(SourceStatus.Offline);
                FD.SetFailReason( "Received Death Message, seq=" + seqNo.ToString() );
            }
            else
            {
                // Unknown device disconnecting, just log to file
                LogAndEvent("Received LWT for unknown device: " + NodeDeviceId);
            }
        }

		// Incoming Birth Cert
        private void ProcessInStatus(string NodeDeviceId, byte[] data)
        {
            Log("ProcessInStatus");
            DrvCSScanner FD;

			// To debug the payload content
			//Console.Write("Payload: ");
			//foreach ( byte b in data)
			//{
			//	Console.Write(b.ToString() + " ");
			//}
			//Console.WriteLine();

			// Interpret ProtoBuf
			Payload ConfigPayload;
			try
			{
				ConfigPayload = Payload.Parser.ParseFrom(data);
			}
			catch (Exception e)
            {
                if (DeviceIndex.TryGetValue(NodeDeviceId, out FD))
                {
                    App.SendReceiveObject(FD.DBScanner.Id, OPCProperty.SendRecFDProtocolError, "Error interpreting Birth message for known: " + NodeDeviceId + " " + e.Message);
                    return;
                }
                else
                {
                    // Invalid JSON for a new device
                    App.SendReceiveObject(this.DBChannel.Id, OPCProperty.SendRecReportConfigError, "Failed to interpret Birth message for unknown: " + NodeDeviceId + " " + e.Message);
                    return;
                }
            }
			LogAndEvent("Birth: " + NodeDeviceId + ", Timestamp: " + ConfigPayload.Timestamp.ToString() + " Sequence: " + ConfigPayload.Seq.ToString());
			
            // A device has connected
            if (DeviceIndex.TryGetValue( NodeDeviceId, out FD))
            {
				// Got the birth message for a previously known device

				// Update version etc in the database objects
				// e.g. if we get a new config version we'll need to request it.

				object ReplyObject = "";

				// Handle the sequence numbers
				App.SendReceiveObject(FD.DBScanner.Id, OPCProperty.SendRecProcessBCStatus, data, ref ReplyObject);

				// Clear alarm
				Log("Clear Scanner (FD) Alarm.");
				App.SendReceiveObject(FD.DBScanner.Id, OPCProperty.SendRecClearScannerAlarm, null);

				// Mark as Online!
				FD.SetStatus(SourceStatus.Online);

				// Existing device may need reconfiguration, so check and do
				if ( (string)ReplyObject == "Yes")
				{
					FD.LogAndEvent("Received newer configuration" );
					HandleNewConfig(ConfigPayload, FD);
				}
				// Birth messages can contain data, so buffered the payload from the Birth message
				// This will get picked up in the device's OnScan method.
				if (dataBuf.ContainsKey(NodeDeviceId))
				{
					LogAndEvent("Removing earlier data from data buffer.");
					dataBuf.Remove(NodeDeviceId);
				}
				dataBuf.Add( NodeDeviceId, ConfigPayload);

				// Finally reset the sequence numbers
				FD.rx_seq = (Int16)ConfigPayload.Seq;
				FD.tx_seq = 0;

			}
			else
            {
                // Brand new device - not seen. 
                LogAndEvent("Device not in database.");
                configItem thisItem;
                if (!configBuf.TryGetValue( NodeDeviceId, out thisItem))
                {
                    LogAndEvent("Device not buffered, adding to memory.");
                    thisItem = new configItem(NodeDeviceId, ConfigPayload);
                    configBuf.Add( NodeDeviceId, thisItem);
                }
				CheckReadyInitiateConfig(NodeDeviceId, thisItem);
			}
        }


		private void HandleNewConfig( Payload P, DrvCSScanner FD)
		{
			FD.LogAndEvent("Request configuration update action within driver.");
			string ErrorText;
			if (UpdateFieldDevice( P, FD, out ErrorText))
			{
				FD.LogAndEvent("Success modifying: " + FD.FullName );
			}
			else
			{
				FD.LogAndEvent("Failed to modify: " + FD.FullName  + " " + ErrorText);
				// Should send receive to raise alarm.
				App.SendReceiveObject(this.DBChannel.Id, OPCProperty.SendRecReportConfigError, "Failed to modify Configuration: " + FD.FullName + " " + ErrorText);
			}
		}

		private void CheckReadyInitiateConfig(string NodeDeviceId, configItem thisItem)
        {
            // Do we have all 3 properties? Only check when Config is received.
            if (thisItem.ready())
            {
                // Allow this to be auto-configured if the broker configuration permits it.
                // Or raise an alert/notification so the user can manually initiate the process
                if (DBChannel.AutoConfig)
                {
                    // Send request to configure. Ends up calling a method with the arguments in this driver exe.
                    LogAndEvent("Request configuration action within driver.");
                    string ErrorText;
                    if (CreateFieldDevice(NodeDeviceId, out ErrorText))
                    {
                        LogAndEvent("Success creating: " + NodeDeviceId );
                    }
                    else
                    {
                        LogAndEvent("Failed to create: " + NodeDeviceId + " " + ErrorText);
                        // Should send receive to raise alarm.
                        App.SendReceiveObject(this.DBChannel.Id, OPCProperty.SendRecReportConfigError, "Failed to create FD: " + NodeDeviceId + " " + ErrorText);
                    }
                    // Remove from queue
                    configBuf.Remove(NodeDeviceId);
                }
                else
                {
                    // Send request to alert user of config need. Ends up calling same CreateFieldDevice method as above
                    LogAndEvent("Request configuration request alarm for: " + NodeDeviceId );
                    App.SendReceiveObject(this.DBChannel.Id, OPCProperty.SendRecRequestConfiguration, NodeDeviceId);
                }
                // Update servers list of items waiting for config.
                RefreshPendingQueue();
            }
        }


        private void ProcessInData(string NodeDeviceId, byte[] data)
        {
            LogAndEvent("ProcessInData");
            DrvCSScanner FD;
            if (DeviceIndex.TryGetValue(NodeDeviceId, out FD))
            {
				// Interpret ProtoBuf
				Payload DataPayload;
				try
				{
					DataPayload = Payload.Parser.ParseFrom(data);
				}
				catch (Exception e)
				{
					FD.LogAndEvent("Error interpreting Data Payload. " + e.Message);
					App.SendReceiveObject(FD.DBScanner.Id, OPCProperty.SendRecFDProtocolError, "Error interpreting Data message for: " + NodeDeviceId);
					FD.LogAndEvent("Completed end receive error interpreting data. ");
					return;
                }

				// Sequence numbers
				FD.rx_seq = (Int16)((FD.rx_seq + 1) % 256);
				if (FD.rx_seq  != (Int16)DataPayload.Seq)
				{
					// Mismatch alarm
					FD.LogAndEvent("Mismatched sequence - was: " + DataPayload.Seq.ToString() + " Should be: " + FD.rx_seq.ToString());
					// Raise alarm?

					// Reset sequence
					FD.rx_seq = (Int16)DataPayload.Seq;
				}

				// Any data?
				if (DataPayload.Metrics.Count == 0)
				{
					FD.LogAndEvent("Empty data message. ");
					return;
				}
				FD.LogAndEvent("Interpreted data Payload OK. ");
				FD.ProcessMetrics( DataPayload);
            }
            else
            {
                LogAndEvent("Cannot find device for this Data. " + NodeDeviceId);
            }
        }


		public override void OnExecuteAction(ClearSCADA.DriverFramework.DriverTransaction Transaction)
        {
			LogAndEvent("Driver Action - channel.");
			switch (Transaction.ActionType)
			{
				case OPCProperty.DriverActionInitiateConfig:
					{
						// Configure a device with this uuid
						string NodeDeviceId = (string)Transaction.get_Args(0);
						string ErrorText;
						if (CreateFieldDevice(NodeDeviceId, out ErrorText))
						{
							this.CompleteTransaction(Transaction, 0, "Successfully created device: " + NodeDeviceId);
						}
						else
						{
							LogAndEvent("Failed to create: " + NodeDeviceId + " " + ErrorText);
							// Should send receive to raise alarm.
							App.SendReceiveObject(this.DBChannel.Id, OPCProperty.SendRecReportConfigError, "Failed to create: " + NodeDeviceId + " " + ErrorText);

							this.CompleteTransaction(Transaction, 0, "Failed to create device: " + NodeDeviceId + " " + ErrorText);
						}
					}
					break;
				case OPCProperty.DriverActionResetConfig:
					{
						LogAndEvent("Reset all configuration versions");
						// For each device, clear the config checksum
						// So when they are read next time they get processed as new
						// I don't think this is needed for Sparkplug !! 
						foreach ( var dev in DeviceIndex)
						{
							DrvCSScanner FD = dev.Value;
							FD.LogAndEvent("Reset " + dev.Key + " Local configuration version");
						}
						this.CompleteTransaction(Transaction, 0, "Successfully reset in driver." );
					}
					break;
				default:
					base.OnExecuteAction(Transaction);
					break;
			}
		}

		public bool UpdateFieldDevice(Payload jc, DrvCSScanner FD, out string ErrorText)
		{
			// Find the device
			// Need to connect to the database as a client
			ClearScada.Client.Simple.Connection connection;
			ClearScada.Client.Advanced.IServer AdvConnection;
			if (!Connect2Net(DBChannel.ConfigUserName, DBChannel.ConfigPass, out connection, out AdvConnection))
			{
				ErrorText = ("Driver cannot connect client to server.");
				return false;
			}

			ClearScada.Client.Simple.DBObject FieldDevice;
			try
			{
				ObjectId FieldENodeId = new ObjectId(unchecked((int)FD.DBScanner.Id));
				FieldDevice = connection.GetObject(FieldENodeId);
			}
			catch (Exception Fail)
			{
				ErrorText = "Cannot find device from its Id? " + Fail.Message;
				return false;
			}
			// Get parent group
			ClearScada.Client.Simple.DBObject ParentGroup = FieldDevice.Parent;

			// Call function to do this all, starting with the UUID
			Log("Reconfigure Device And Children");
			return ReconfigureDeviceAndChildren( jc, FieldDevice, ParentGroup, connection, AdvConnection, out ErrorText);
		}

		public bool CreateFieldDevice(string NodeDeviceId, out string ErrorText)
		{
			// Need to connect to the database as a client
			ClearScada.Client.Simple.Connection connection;
			ClearScada.Client.Advanced.IServer AdvConnection;
			if (!Connect2Net(DBChannel.ConfigUserName, DBChannel.ConfigPass, out connection, out AdvConnection))
			{
				ErrorText = ("Driver cannot connect client to server.");
				return false;
			}

			configItem thisItem;
			if (!configBuf.TryGetValue(NodeDeviceId, out thisItem))
			{
				ErrorText = ("Cannot find device config message in configuration buffer.");
				return false;
			}

			bool status =  CreateFieldDeviceObjects(NodeDeviceId, thisItem.birthData, connection, AdvConnection, out ErrorText);
			// Should disconnect everywhere it goes wrong too!!
			DisconnectNet(connection, AdvConnection);

			// There may be data in the metrics, so buffer this for processing after device creation
			// (OnDefine needs to have been called).
			if (dataBuf.ContainsKey( NodeDeviceId))
			{
				LogAndEvent("Removing earlier data from data buffer");
				dataBuf.Remove(NodeDeviceId);
			}
			dataBuf.Add(NodeDeviceId, thisItem.birthData);
			LogAndEvent("Add to data buffer");
			// Finish by remove 
			configBuf.Remove(NodeDeviceId);
			LogAndEvent("Removed from config buffer");
			RefreshPendingQueue();
			LogAndEvent("Config buffer refreshed.");

			return status;
		}

		Int32 QueryDatabaseForId(ClearScada.Client.Advanced.IServer AdvConnection, string sql)
		{
			Int32 result = 0;

			// Find the database reference of the Node
			ClearScada.Client.Advanced.IQuery serverQuery = AdvConnection.PrepareQuery(sql, new ClearScada.Client.Advanced.QueryParseParameters());
			ClearScada.Client.Advanced.QueryResult queryResult = serverQuery.ExecuteSync(new ClearScada.Client.Advanced.QueryExecuteParameters());

			if (queryResult.Status == ClearScada.Client.Advanced.QueryStatus.Succeeded || queryResult.Status == ClearScada.Client.Advanced.QueryStatus.NoDataFound)
			{
				if (queryResult.Rows.Count > 0)
				{
					// Found
					IEnumerator<ClearScada.Client.Advanced.QueryRow> e = queryResult.Rows.GetEnumerator();
					while (e.MoveNext())
					{
						result = (Int32)e.Current.Data[0];
					}
				}
			}
			serverQuery.Dispose();

			return result;
		}

		public bool CreateFieldDeviceObjects(	string NodeDeviceId, 
												Payload birthData,
												ClearScada.Client.Simple.Connection connection, 
												ClearScada.Client.Advanced.IServer AdvConnection, 
												out string ErrorText)
		{
			// Node and Device are separate objects.
			// Code assumes we get Node info first and it is configured before its devices
			string[] NodeDeviceParts = NodeDeviceId.Split('/');
			string NodeId = NodeDeviceParts[0];
			string DeviceId = "";
			if ( NodeDeviceParts.Length > 1)
			{
				DeviceId = NodeDeviceParts[1];
			}
			// SCADA object names may not contain invalid chars
			string GSNodeId = Sp2GSname(NodeId);
			string GSDeviceId = Sp2GSname(DeviceId);
			string GSNodeDeviceId = GSNodeId + ((GSDeviceId== "") ? "" : "." + GSDeviceId);

			// Define FieldDevice here
			ClearScada.Client.Simple.DBObject FieldDevice = null;
			// The group our Field Device sits in
			ClearScada.Client.Simple.DBObject Instance = null;
			// We need to get this to be able to create an instance or a group to contain the Node or Device
			Int32 ParentNodeId = 0;

			// Check does not exist
			// Need to query all devices on this channel by ENodeId
			// Enclose scope so the same names can be used
			string sql = "SELECT Id, Fullname FROM SparkplugBND WHERE ChannelId = " + DBChannel.Id.ToString() + 
							" AND NodeDevice = '" + NodeDeviceId + "'";
			if ( QueryDatabaseForId(AdvConnection, sql) > 0)
			{ 
				ErrorText = ("A device with this Node/Device Id exists: " + NodeDeviceId);
				return false;
			}
			else
			{
				LogAndEvent("No existing device found - proceeding to create: " + NodeDeviceId);
			}

			// Find Node reference if this is a device - creating a Device will assume a Node already exists, if not we have to create one
			if ( NodeDeviceId.Contains( "/") )
			{
				sql = "SELECT id, Fullname, NodeDevice FROM SparkplugBND WHERE ChannelId = " + DBChannel.Id.ToString() +
							" AND NodeDevice = '" + NodeId + "'";

				ParentNodeId = QueryDatabaseForId( AdvConnection, sql);

				// We have a parent node to link to?
				if (ParentNodeId == 0)
				{
					LogAndEvent("Not Found Parent Id" );
					// No, so must be instance Node - do this recursively with the NodeId and an empty payload
					Payload EmptyParent = new Payload();
					EmptyParent.Timestamp = birthData.Timestamp;
					if ( !CreateFieldDeviceObjects( NodeId, EmptyParent, connection, AdvConnection, out ErrorText))
					{
						ErrorText += " (creating empty parent Node)";
						return false;
					}
					// Code below needs the Id of the newly created parent.
					// Find the database reference of the Node
					sql = "SELECT id, Fullname, NodeDevice FROM SparkplugBND WHERE ChannelId = " + DBChannel.Id.ToString() +
									" AND NodeDevice = '" + NodeId + "'";

					ParentNodeId = QueryDatabaseForId(AdvConnection, sql);

					if (ParentNodeId != 0)
					{
						LogAndEvent("Found parent node: " + ParentNodeId.ToString());
					}
					else
					{
						ErrorText = "Still can't find Node after creation attempt.";
						return false;
					}
				}
			}

			// Construct name of instance from the Configuration
			// If this is a device, then name is <ConfigGroupId>.<NodeId>.Node
			//                                or <ConfigGroupId>.<NodeId>.<DeviceId>.Device

			// Could also filter other invalid characters. Used for devices too, if no template.
			LogAndEvent("Configure device: " + NodeDeviceId + " as " + GSNodeDeviceId);

			// Group to contain instances or field device object groups
			ClearScada.Client.Simple.DBObject InstanceGroup;
			if (DBChannel.ConfigGroupId.Id <= 0)
			{
				ErrorText = ("Invalid channel instance group configuration.");
				return false;
			}
			try
			{
				InstanceGroup = connection.GetObject((ClearScada.Client.ObjectId)(int)DBChannel.ConfigGroupId.Id);
				LogAndEvent("Instance group. " + InstanceGroup.FullName);
			}
			catch
			{
				ErrorText = ("Invalid Node group. ");
				return false;
			}

			// If this is a Device, then try to get the Node
			// Group will commonly be named <chosen node group>.<node name> - Which is InstanceGroup.GetChild(GSNodeId);
			// But user could have moved items, so better to use:
			// <group the node object belongs to = ParentNodeId.Parent.FullName>
			ClearScada.Client.Simple.DBObject NodeGroup = null;
			if (GSDeviceId != "")
			{
				try
				{
					ObjectId ParentNodeObjectId = new ObjectId(ParentNodeId);
					ClearScada.Client.Simple.DBObject ParentNodeObject = connection.GetObject(ParentNodeObjectId);
					NodeGroup = ParentNodeObject.Parent; 
					LogAndEvent("Node group. " + NodeGroup.FullName);
				}
				catch
				{
					ErrorText = ("Cannot find device group.");
					return false;
				}
			}

			// Read the template information from the channel
			// Use unchecked type conversion as there are differences in DDK library vs Client library in handling object ids
			Int32 TemplateId = unchecked((int)DBChannel.TemplateId.Id);
			// This is what we use if it's a Node. If a Device then try to read a device template
			if (GSDeviceId != "")
			{
				Int32 DeviceTemplatesId = unchecked((int)DBChannel.DeviceTemplatesId.Id);
				if (DeviceTemplatesId >0)
				{
					try
					{
						// Read the group
						ClearScada.Client.Simple.DBObject DeviceTemplateGroup = connection.GetObject(DeviceTemplatesId);
						// Try to read a Template
						// TODO It would be more appropriate here to search for the Device template using the DeviceId property of its contained Device object
						ClearScada.Client.Simple.DBObject DeviceTemplate = connection.GetObject(DeviceTemplateGroup.FullName + "." + GSDeviceId);
						if (DeviceTemplate == null)
						{
							LogAndEvent("Cannot find Device Template.");
							TemplateId = 0;
						}
						else
						{
							LogAndEvent("Using Device Template. " + DeviceTemplate.FullName);
							TemplateId = DeviceTemplate.Id;
						}
					}
					catch
					{
						LogAndEvent("Cannot read Device Template.");
						TemplateId = 0;
					}
				}
				else
				{
					LogAndEvent("No group of Device templates configured.");
					TemplateId = 0;
				}
			}

			// Create instance or group with suggested name
			LogAndEvent("Trying to create instance or group.");
			if (TemplateId >0)
			{
				LogAndEvent("Valid TemplateId");
				ObjectId CSTemplateId = new ObjectId(TemplateId);

				// Create Instance
				try
				{
					// Create instance of node/device template
					if (GSDeviceId == "")
					{
						Instance = connection.CreateInstance(CSTemplateId, InstanceGroup.Id, GSNodeId);
					}
					else
					{
						Instance = connection.CreateInstance(CSTemplateId, NodeGroup.Id, GSDeviceId);
					}
				}
				catch (Exception Fail)
				{
					ErrorText = ("Error creating device template instance. " + Fail.Message);
					// This could raise an alarm - there should not be an object with this name.
					return false;
				}
				// Now find the FD associated with this template
				// Find the FD - assumed to be a direct child of the instance
				ClearScada.Client.Simple.DBObjectCollection fds = Instance.GetChildren("", "");
				bool Found = false;
				foreach (ClearScada.Client.Simple.DBObject o in fds)
				{
					if (o.ClassDefinition.Name == "SparkplugBND")
					{
						// This is an FD - assume only one
						Found = true;
						LogAndEvent("Found FD: " + o.FullName);
						FieldDevice = o;
						break;
					}
				}
				if (!Found)
				{
					ErrorText = ("No FD found as child of Template instance.");
					return false;
				}
			}
			else
			{
				// There is no template for this profile, so create FD in a group directly
				// Not an instance, but using that object ref
				// In the case of a Device, we need device group in node group
				LogAndEvent("Create group directly.");
				try
				{
					if (GSDeviceId == "")
					{
						Instance = connection.CreateObject("CGroup", InstanceGroup.Id, GSNodeId);
					}
					else
					{
						Instance = connection.CreateObject("CGroup", NodeGroup.Id, GSDeviceId);
					}
				}
				catch (Exception Failure)
				{
					// Possible to consider continuing here, keep this group and add FD to it.
					ErrorText = "Group already exists - cannot create field device. " + Failure.Message;
					return false;
				}
				// Create field device
				try
				{
					string FDName = "FD";
					if (GSDeviceId == "")
					{
						FDName = "Node";
					}
					else
					{
						FDName = "Device";
					}
					FieldDevice = connection.CreateObject("SparkplugBND", Instance.Id, FDName); // Or name 'instancename' ?
				}
				catch (Exception Failure)
				{
					ErrorText = "Field Device already exists. " + Failure.Message;
					return false;
				}
			}
			// Creating Field Device etc.

			// Set field device properties
			// ChannelId - ignore if set already - may be already set appropriately in the template

			if (! CheckSet( FieldDevice, "ENodeId", NodeId))
			{
				ErrorText = "Error writing device ENodeId.";
				return false;
			}

			if (FieldDevice.Id.ToInt32() == (Int32)this.DBChannel.Id)
			{
				LogAndEvent("Field Device has correct channel.");
			}
			else
			{
				if (!CheckSet(FieldDevice, "ChannelId", this.DBChannel.Id))
				{
					ErrorText = "Error writing device ChannelId.";
					return false;
				}
			}

			// DeviceId. Checkset returns false if failed, 
			if (!CheckSet(FieldDevice, "DeviceId", DeviceId))
			{
				ErrorText = "Error writing device DeviceId.";
				return false;
			}


			// Parent Node if this is a device
			if (DeviceId != "")
			{
				CheckSet(FieldDevice, "ParentNodeId", ParentNodeId);
			}

			// Set other properties via SendReceive

			// Use DLL to write database fields
			string Reply = "";
			object ReplyObject = Reply;
			LogAndEvent("Send/receive Birth Status");
			byte[] bytes;
			using (System.IO.MemoryStream stream = new System.IO.MemoryStream())
			{
				// Save the person to a stream
				birthData.WriteTo(stream);
				bytes = stream.ToArray();
			}
			App.SendReceiveObject((uint)FieldDevice.Id.ToInt32(), OPCProperty.SendRecProcessBCStatus, bytes, ref ReplyObject); // Birth data.

			// Set properties function
			bool status = ReconfigureDeviceAndChildren(birthData, FieldDevice, Instance, connection, AdvConnection, out ErrorText);
			if (!status)
			{
				return false;
			}

			//if good
			ErrorText = "";
            return true;                  
        }

		// Return our driver point table names suitable for the type requested
		// Several types may be returned. The first is used by default. 
		// This is used in the SQL query to find the points.
		private string GetSparkplugTypeName( uint DataType)
		{
			string csType;
			switch (DataType)
			{
				case 0:
					// Unknown
					csType = "";
					break;
				case 1:
					csType = "'SparkplugBPointAg','SparkplugBPointDg'";
					// Could be a 2 or 3 bit digital too.
					// Int8
					// Signed 8-bit integer
					// Google Protocol Buffer Type: uint32
					break;
				case 2:
					csType = "'SparkplugBPointAg'";
					// Int16
					// Signed 16-bit integer
					// Google Protocol Buffer Type: uint32
					break;
				case 3:
					csType = "'SparkplugBPointAg','SparkplugBPointCi'";
					// Int32
					// Signed 32-bit integer
					// Google Protocol Buffer Type: uint32
					break;
				case 4:
					csType = "'SparkplugBPointAg','SparkplugBPointCi'";
					// Int64
					// Signed 64-bit integer
					// Google Protocol Buffer Type: uint64
					break;
				case 5:
					csType = "'SparkplugBPointAg'";
					// Uint8
					// Unsigned 8-bit integer
					// Google Protocol Buffer Type: uint32
					break;
				case 6:
					csType = "'SparkplugBPointAg','SparkplugBPointCi'";
					// Uint16
					// Unsigned 16-bit integer
					// Google Protocol Buffer Type: uint32
					break;
				case 7:
					csType = "'SparkplugBPointAg','SparkplugBPointCi'";
					// Uint32
					// Unsigned 32-bit integer
					// Google Protocol Buffer Type: uint32
					break;
				case 8:
					csType = "'SparkplugBPointAg','SparkplugBPointCi'";
					// Uint64
					// Unsigned 64-bit integer
					// Google Protocol Buffer Type: uint64
					break;
				case 9:
					csType = "'SparkplugBPointAg'";
					// Float
					// 32-bit floating point number
					// Google Protocol Buffer Type: float
					break;
				case 10:
					csType = "'SparkplugBPointAg'";
					// Double
					// 64-bit floating point number
					// Google Protocol Buffer Type: double
					break;
				case 11:
					csType = "'SparkplugBPointDg'";
					// Boolean
					// Boolean value
					// Google Protocol Buffer Type: bool
					break;
				case 12:
					csType = "'SparkplugBPointSt'";
					// String
					// String value (UTF-8)
					// Google Protocol Buffer Type: string
					break;
				case 13:
					csType = "'SparkplugBPointTm'";
					// DateTime
					// Date time value as uint64 value representing milliseconds since epoch (Jan 1, 1970)
					// Google Protocol Buffer Type: uint64
					break;
				case 14:
					csType = "'SparkplugBPointSt'";
					// Text
					// String value (UTF-8)
					// Google Protocol Buffer Type: string
					break;

				// Custom Types
				case 15:
					csType = "'SparkplugBPointSt'";
					// UUID
					// UUID value as a UTF-8 string
					// Google Protocol Buffer Type: string
					break;
				case 16:
					csType = "";
					// DataSet
					// DataSet as defined in section 3.1.7
					// Google Protocol Buffer Type: none – defined in Sparkplug
					break;
				case 17:
					csType = "";
					// Bytes
					// Array of bytes
					// Google Protocol Buffer Type: bytes
					break;
				case 18:
					csType = "";
					// File
					// Array of bytes representing a file
					// Google Protocol Buffer Type: bytes
					break;
				case 19:
					csType = "";
					// Template
					// Template as defined in section 3.1.10
					// Google Protocol Buffer Type: none – defined in Sparkplug
					break;
				default:
					csType = "";
					break;
			}
			return csType;
		}

		private bool ReconfigureDeviceAndChildren(	Payload jc, 
													ClearScada.Client.Simple.DBObject FieldDevice,
													ClearScada.Client.Simple.DBObject ParentInstance,
													ClearScada.Client.Simple.Connection connection,
													ClearScada.Client.Advanced.IServer AdvConnection,
													out string ErrorText)
		{
			ErrorText = "";

			// Create points
			if (jc.Metrics.Count > 0)
			{
				foreach (Payload.Types.Metric p in jc.Metrics)
				{
					// Should we ignore the "bdSeq" metric? Should only get this in a NDEATH message

					// Read from the list of Metric Type numbers
					// Available types:
					//csType = "SparkplugBPointAg";
					//csType = "SparkplugBPointCi"; // May not end up getting used, at least automatically
					//csType = "SparkplugBPointDg";
					//csType = "SparkplugBPointSt";
					//csType = "SparkplugBPointTm";
					string csType = "";
					csType = GetSparkplugTypeName(p.Datatype);

					if (csType == "")
					{
						LogAndEvent("Unsupported or unspecified point type. " + p.Datatype.ToString());
						continue;
					}

					LogAndEvent("Find/Create: Type: " + csType + ", " + ", Name: '" + p.Name + "', Address: " + p.Alias.ToString() + ", Parent: " + FieldDevice.FullName);

					// If there is a point of the correct type and SparkplugName linked to this device, use it
					// or
					// If there is a point of the correct type and SparkplugName not linked to this device, link and use it
					// or
					// If there is a point with name <ConfigGroupId>.<Node>[.Device].<Name/path>, use it (it could have been created in a previous template)
					// or
					//   Create standalone point
					//     <ConfigGroupId>.<Node>[.Device].<Name/path>

					// SP names use / as folder separators, and may include . which can't be used
					// We will try to replace . with _ and then replace / with .
					// But we also use a function to replace other non-allowed characters with _
					string GSPointPath = (Sp2GSname( p.Name)).Replace( "/", ".");
					string pointFullName = FieldDevice.Parent.FullName + "." + GSPointPath; // 'Ideal' point name
					string PointTables = "SparkplugBPointAg UNION SparkplugBPointDg UNION SparkplugBPointCi UNION SparkplugBPointTm UNION SparkplugBPointSt";

					// The point we are looking for
					Int32 pointId = 0;

					if (pointId == 0)
					{
						// If there is a point of the correct type and SparkplugName linked to this device, use it
						// Look for a point of type csType = SPtype and p.name = SparkplugName linked to this FieldDevice, so it could be in a different folder
						string sql = "SELECT id, Fullname, ScannerId, SparkplugName, SPType, Address FROM " + PointTables + " WHERE " +
										"ScannerId = " + FieldDevice.Id.ToString() + " and " +
										"SparkplugName = '" + p.Name + "' and " +
										"TypeName in (" + csType + ")";
						pointId = QueryDatabaseForId(AdvConnection, sql);
						if (pointId != 0)
						{
							LogAndEvent("Found point 1st time");
						}
					}

					// Not found a point with the right SparkplugName, so look in folder
					// If there is a point of the correct type and SparkplugName not linked to this device, link and use it
					if (pointId == 0)
					{
						// Look for a point of type csType = SPtype and p.name = SparkplugName in FieldDevice folder/subfolders
						string sql = "SELECT id, Fullname, ScannerId, SparkplugName, SPType, Address FROM " + PointTables + " WHERE " +
										"FullName LIKE '" + FieldDevice.Parent.FullName + ".%' and " +
										"SparkplugName = '" + p.Name + "' and " +
										"TypeName in (" + csType + ")";
						pointId = QueryDatabaseForId(AdvConnection, sql);
						if (pointId != 0)
						{
							LogAndEvent("Found point 2nd time");
						}
					}

					// Still not found,
					// If there is a point with name <ConfigGroupId>.<Node>[.Device].<Name/path>, use it 
					// (it could have been created in a previous template and the SP Name not set)
					if (pointId == 0)
					{
						// Look for a point of type csType = SPtype and p.name = SparkplugName in FieldDevice folder/subfolders
						string sql = "SELECT id, Fullname, ScannerId, SparkplugName, SPType, Address FROM " + PointTables + " WHERE " +
										"FullName = '" + FieldDevice.Parent.FullName + "." + pointFullName + "' and " +
										"TypeName in (" + csType + ")";

						pointId = QueryDatabaseForId(AdvConnection, sql);
						if (pointId != 0)
						{
							LogAndEvent("Found point 3rd time");
						}
					}

					// Last chance - create point
					// First work out the desired class name - first of the comma sep quoted list in csType
					string[] PointTypeList = csType.Split(',');
					string DesiredType = PointTypeList[0].Replace("'","");
					ClearScada.Client.Simple.DBObject PointObject;
					if (pointId == 0)
					{
						// First create any parent subfolders specified by the Name property
						string[] GSPointPathList = GSPointPath.Split('.');
						ClearScada.Client.Simple.DBObject ParentGroup = FieldDevice.Parent;
						// Process each element except the last, which is the point name
						for (int i = 0; i < GSPointPathList.Length - 1; i++)
						{
							// Does it exist already?
							ClearScada.Client.Simple.DBObject ChildGroup;
							try
							{
								ChildGroup = ParentGroup.GetChild(GSPointPathList[i]);
							}
							catch (Exception e)
							{
								LogAndEvent("Error finding child group: " + GSPointPathList[i] + ", " + e.Message);
								break;
							}
							if (ChildGroup == null)
							{
								try
								{
									// Create group
									ChildGroup = ParentGroup.CreateObject("CGroup", GSPointPathList[i]);
								}
								catch (Exception e)
								{
									LogAndEvent("Error creating child group: " + GSPointPathList[i] + ", " + e.Message);
									break;
								}
							}
							// Rinse and repeat
							ParentGroup = ChildGroup;
						}

						// No valid Parent
						if (ParentGroup == null)
						{
							LogAndEvent("Cannot find parent group of point.");
							continue;
						}

						// Create the point and get its Id
						try
						{
							LogAndEvent("Create point");
							PointObject = connection.CreateObject(DesiredType, ParentGroup.Id, GSPointPathList[GSPointPathList.Length-1]);
							pointId = PointObject.Id.ToInt32();
							Log("Created point");
						}
						catch (Exception Fail)
						{
							LogAndEvent("Cannot create point: " + GSPointPathList[GSPointPathList.Length - 1] + " Error: " + Fail.Message);
							continue;
						}
					}
					else
					{
						LogAndEvent("Get point from reference");
						ObjectId CSPointId = new ObjectId(pointId);
						PointObject = connection.GetObject(CSPointId);
					}
					// Get the actual type found/created
					csType = (string) PointObject["TypeName"];

					// Update the point properties
					LogAndEvent("Set point properties");
					CheckSet( PointObject, "ScannerId", FieldDevice.Id.ToInt32());
					CheckSet( PointObject, "Address", p.Alias);
					// Set historic filters and enable history
					// TODO - here and elsewhere only set these on first create?
					CheckSet(PointObject,"HistoricFilterValue", true);
					CheckSet(PointObject,"HistoricFilterState", true);
					CheckSet(PointObject,"HistoricFilterReport", true);
					CheckSet(PointObject,"HistoricFilterEOP", true);
					
					// Historic - enable by default
					try
					{
						// Set historic
						PointObject.Aggregates["Historic"].Enabled = true;
					}
					catch (Exception Failure)
					{
						LogAndEvent("Error writing point history aggregate. " + Failure.Message);
					}

					CheckSet(PointObject, "InService", true);

					// Point Name
					CheckSet(PointObject, "SparkplugName", p.Name);
					// Type number
					CheckSet(PointObject, "SPtype", p.Datatype);

					// If output set control  aggregate properties
					// No metadata from the protocol tells us we can do this

					// Individual point types - adjust default settings if needed
					// Use the Properties part of a metric to set Geo SCADA fields
					// This could in fact be sensitive to the schema and match fields automatically
					if (p.Properties != null)
					{
						for (int i = 0; i < p.Properties.Keys.Count; i++)
						{
							string KeyName = p.Properties.Keys[i];
							Payload.Types.PropertyValue PropValue = p.Properties.Values[i];
							uint dataType = PropValue.Type; // Using payload type numbers

							if (KeyName == "Units" && dataType == 12 && (csType == "SparkplugBPointAg" || csType == "SparkplugBPointCi"))
							{
								CheckSet( PointObject, "Units", PropValue.StringValue);
							}

							if (KeyName == "FullScale" && dataType == 10 && csType == "SparkplugBPointAg")
							{
								CheckSet( PointObject, "FullScale", PropValue.DoubleValue);
							}

							if (KeyName == "ZeroScale" && dataType == 10 && csType == "SparkplugBPointAg")
							{
								CheckSet( PointObject, "ZeroScale", PropValue.DoubleValue);
							}
						}
					}
					// The following could also be added to this behaviour
					//PointObject.SetProperty("SigChangeValue", a.sigChangeValue);
					//CheckSet(PointObject, "HighSeverityType", l.action);
					//CheckSet(PointObject, "HighLimitStd", l.value);
					//CheckSet(PointObject, "HighDesc", l.name);
					//CheckSetIfZero(PointObject, "HighSeverity", 334);
					//CheckSet(PointObject, "HighHighSeverityType", l.action);
					//CheckSet(PointObject, "HighHighLimitStd", l.value);
					//CheckSet(PointObject, "HighHighDesc", l.name);
					//CheckSetIfZero(PointObject, "HighHighSeverity", 334);
					//CheckSet(PointObject, "LowSeverityType", l.action);
					//CheckSet(PointObject, "LowLimitStd", l.value);
					//CheckSet(PointObject, "LowDesc", l.name);
					//CheckSetIfZero(PointObject, "LowSeverity", 334);
					//CheckSet(PointObject, "LowLowSeverityType", l.action);
					//CheckSet(PointObject, "LowLowLimitStd", l.value);
					//CheckSet(PointObject, "LowLowDesc", l.name);
					//CheckSetIfZero(PointObject, "LowLowSeverity", 334);
					//CheckSet(PointObject, "Scale", a.scale);
					//CheckSet(PointObject, "ResetPeriod", a.resetPeriod);
					//CheckSet(PointObject, "ResetOffset", a.resetOffset);
					//CheckSet(PointObject, "ReportPeriod", a.reportPeriod);
					//CheckSet(PointObject, "BitCount", 2);
					//CheckSet(PointObject, "State0Desc", d.state0Name);
					//CheckSet(PointObject, "State0Action", d.state0Action);
					//CheckSet(PointObject, "State0Pers", d.state0Pers);
					//CheckSet(PointObject, "State0SeverityType", d.state0Action); // Also try writing here
					//CheckSetIfZero(PointObject, "State0Severity", 334);
					//CheckSet(PointObject, "State1Desc", d.state1Name);
					//CheckSet(PointObject, "State1Action", d.state1Action);
					//CheckSet(PointObject, "State1Pers", d.state1Pers);
					//CheckSet(PointObject, "State1SeverityType", d.state0Action); // Also try writing here
					//CheckSetIfZero(PointObject, "State1Severity", 334);
					//CheckSet(PointObject, "State2Desc", d.state2Name);
					//CheckSet(PointObject, "State2Action", d.state2Action);
					//CheckSet(PointObject, "State2Pers", d.state2Pers);
					//CheckSet(PointObject, "State2SeverityType", d.state0Action); // Also try writing here
					//CheckSetIfZero(PointObject, "State2Severity", 334);

					//CheckSet(PointObject, "State3Desc", d.state3Name);
					//CheckSet(PointObject, "State3Action", d.state3Action);
					//CheckSet(PointObject, "State3Pers", d.state3Pers);
					//CheckSet(PointObject, "State3SeverityType", d.state0Action); // Also try writing here
					//CheckSetIfZero(PointObject, "State3Severity", 334);

					//CheckSet(PointObject, "State4Desc", d.state4Name);
					//CheckSet(PointObject, "State4Action", d.state4Action);
					//CheckSet(PointObject, "State4Pers", d.state4Pers);
					//CheckSet(PointObject, "State4SeverityType", d.state0Action); // Also try writing here
					//CheckSetIfZero(PointObject, "State4Severity", 334);

					//CheckSet(PointObject, "State5Desc", d.state5Name);
					//CheckSet(PointObject, "State5Action", d.state5Action);
					//CheckSet(PointObject, "State5Pers", d.state5Pers);
					//CheckSet(PointObject, "State5SeverityType", d.state0Action); // Also try writing here
					//CheckSetIfZero(PointObject, "State5Severity", 334);

					//CheckSet(PointObject, "State6Desc", d.state6Name);
					//CheckSet(PointObject, "State6Action", d.state6Action);
					//CheckSet(PointObject, "State6Pers", d.state6Pers);
					//CheckSet(PointObject, "State6SeverityType", d.state0Action); // Also try writing here
					//CheckSetIfZero(PointObject, "State6Severity", 334);

					//CheckSet(PointObject, "State7Desc", d.state7Name);
					//CheckSet(PointObject, "State7Action", d.state7Action);
					//CheckSet(PointObject, "State7Pers", d.state7Pers);
					//CheckSet(PointObject, "State7SeverityType", d.state0Action); // Also try writing here
					//CheckSetIfZero(PointObject, "State7Severity", 334);

				}
			}
			LogAndEvent("All points done");

			// Return from doing device fields and points

			//if good
			if (ErrorText == "")
			{
				return true;
			}
			else
			{
				return false;
			}
		}

		// Generic way to set all object properties
		private bool CheckSet( ClearScada.Client.Simple.DBObject CSObj, string FieldName, ValueType PropValue)
		{
			if (PropValue != null)
			{
				try
				{
					CSObj.SetProperty(FieldName, PropValue);
					return true;
				}
				catch (Exception Failure)
				{
					LogAndEvent("Error writing field " + FieldName + " " + Failure.Message);
				}
			}
			return false;
		}

		private bool CheckSet(ClearScada.Client.Simple.DBObject CSObj, string FieldName, string PropValue)
		{
			if (PropValue != null)
			{
				try
				{
					CSObj.SetProperty(FieldName, PropValue);
					return true;
				}
				catch (Exception Failure)
				{
					LogAndEvent("Error writing field " + FieldName + " " + Failure.Message);
				}
			}
			return false;
		}

		private bool CheckSetIfZero(ClearScada.Client.Simple.DBObject CSObj, string FieldName, ValueType PropValue)
		{
			if (PropValue != null)
			{
				try
				{
					if ((int)CSObj.GetProperty(FieldName) == 0)
					{
						CSObj.SetProperty(FieldName, PropValue);
					}
					return true;
				}
				catch (Exception Failure)
				{
					LogAndEvent("Error writing field " + FieldName + " " + Failure.Message);
				}
			}
			return false;
		}

		private string Sp2GSname( string name)
        {
			return name.Replace('.', '_').Replace('+', '-').Replace('=', '_').Replace('#', '_').Replace('<', '_').Replace('>', '_').Replace('|', '_').Replace('@', '_').Replace(':', '_').Replace(';', '_');
		}

		public void RefreshPendingQueue()
        {
            // Build a string array of uuid and device information
            string [] pendingQueue = new string[configBuf.Count];
            int i = 0;
            foreach ( var c in configBuf)
            {
                pendingQueue[i] = c.Value.NodeDeviceId;
                i++;
            }
            App.SendReceiveObject(DBChannel.Id, OPCProperty.SendRecUpdateConfigQueue, pendingQueue);
        }

        public bool Connect2Net(string user, string password, out ClearScada.Client.Simple.Connection connection, out ClearScada.Client.Advanced.IServer AdvConnection)
        {
            var node = new ClearScada.Client.ServerNode(ClearScada.Client.ConnectionType.Standard, "127.0.0.1", 5481);
            connection = new ClearScada.Client.Simple.Connection("SparkplugB");
            try
            {
                connection.Connect(node);
                AdvConnection = node.Connect("SparkplugB-Adv", false);
            }
            catch (CommunicationsException)
            {
                LogAndEvent("Unable to communicate with ClearSCADA server.");
                AdvConnection = null;
                return false;
            }

            if (connection.IsConnected)
            {
                Log("Connected to database.");

                using (var spassword = new System.Security.SecureString())
                {
                    foreach (var c in password)
                    {
                        spassword.AppendChar(c);
                    }

                    try
                    {
                        connection.LogOn(user, spassword);
                        AdvConnection.LogOn(user, spassword);
                    }
                    catch (AccessDeniedException)
                    {
                        LogAndEvent("Access denied, please check username and password. Check CAPS LOCK is off.");
                        return false;
                    }
                    catch (PasswordExpiredException)
                    {
                        LogAndEvent("Password is expired. Please reset with ViewX or WebX.");
                        return false;
                    }

                    Log("Logged In.");
                    return true;
                }
            }
            else
            {
                return false;
            }
        }

        public bool DisconnectNet( ClearScada.Client.Simple.Connection connection,  ClearScada.Client.Advanced.IServer AdvConnection)
        {
            try
            {
                connection.LogOff();
                connection.Disconnect();
                connection.Dispose();

                AdvConnection.LogOff();
                AdvConnection.Dispose();
            }
            catch (Exception Failure)
            {
                LogAndEvent("Disconnect Error: " + Failure.Message);
                return false;
            }
            return true;
        }
    }

    public class DrvCSScanner : DriverScanner<SparkplugBND>
    {
		// Class variables used for sequence numbering each way
		public Int16 rx_seq;
		public Int16 tx_seq;
		
		// Logging, including server-side debug logging (can add considerable server load)
		public void LogAndEvent(string Message)
		{
			Log(Message);
			// Channel/broker property used to enable enhanced logging
			DrvSparkplugBBroker broker = (DrvSparkplugBBroker)Channel;
			if (broker.DBChannel.EnhancedEvents)
			{
				App.SendReceiveObject(this.DBScanner.Id, OPCProperty.SendRecLogFDEventText, Message);
			}
		}

		public override SourceStatus OnDefine()
        {
			SetScanRate(	DBScanner.NormalScanRate * 1000,
							DBScanner.NormalScanOffset);

			LogAndEvent("Scanner defined");

            ((DrvSparkplugBBroker)Channel).AddScannerToIndex(this);

			// Attempt to process data from queue here
			// Unusual, as being done before setting source status online
			// Also done in OnScan, but that seems to take a long time
			DrvSparkplugBBroker broker = (DrvSparkplugBBroker)Channel;
			Payload DataPayload;
			if (broker.dataBuf.Count > 0)
			{
				LogAndEvent("Data buffer not empty (in OnDefine).");

				if (broker.dataBuf.TryGetValue(this.DBScanner.NodeDevice, out DataPayload))
				{
					LogAndEvent("Process pending data from Birth.");
					ProcessMetrics(DataPayload);

					broker.dataBuf.Remove(this.DBScanner.NodeDevice);
				}
			}
			// Reset sequence numbers
			// It's not fully clear in the Sparkplug B document whether this is correct - for Node and/or Device
			rx_seq = 0;
			tx_seq = 0;

			return SourceStatus.Online;
        }

		public override void OnScan()
		{
			DrvSparkplugBBroker broker = (DrvSparkplugBBroker)Channel;

			// Action flags are only set for existing devices. New ones rely on this list here:
			if (broker.actionBuf.ContainsKey(FullName))
			{
				LogAndEvent("Unbuffering new devices requests.");
				var aflags = broker.actionBuf[FullName];

				// Not needed at present for Sparkplug
				broker.actionBuf.Remove(FullName);
			}

			// This is used to pick up and process pending data, for example the
			// Birth message in Sparkplug can contain data which can't be processed until
			// the objects are all configured.
			// Also done in OnDefine, which generally gets there before this try in OnScan
			Payload DataPayload;
			if (broker.dataBuf.Count > 0)
			{
				LogAndEvent("Data buffer not empty (in OnScan).");

				if (broker.dataBuf.TryGetValue(this.DBScanner.NodeDevice, out DataPayload))
				{
					LogAndEvent("Process pending data from Birth.");
					ProcessMetrics(DataPayload);

					broker.dataBuf.Remove(this.DBScanner.NodeDevice);
				}
			}
		}


		public override void OnUnDefine()
        {
            ((DrvSparkplugBBroker)Channel).RemoveScannerFromIndex(this);
            base.OnUnDefine();
        }

		public override void OnExecuteAction(DriverTransaction Transaction)
		{
			LogAndEvent("Driver Action - scanner.");
			switch (Transaction.ActionType)
			{
				case OPCProperty.DriverActionResetConfigFD:
					{
						// Set the config versions to -1
						// So when they are read next time they get processed as new
						this.DBScanner.ConfigChecksum = "";
						LogAndEvent("Reset Configuration checksum");
					}
					this.CompleteTransaction(Transaction, 0, "Successfully reset checksum.");
					break;

				default:
					base.OnExecuteAction(Transaction);
					break;
			}
		}

		public void ProcessMetrics(Payload DataPayload)
		{
			bool DataReceived = false;

			foreach (Payload.Types.Metric r in DataPayload.Metrics)
			{
				// Points are identified by either "alias":uint64 or by "name":"point name". Use name if it exists
				if ((r.Name == null) || (r.Name == ""))
				{
					// Use the alias
					if (r.Alias != 0)
					{
						ProcessPointByNumber(r.Alias, r);
						DataReceived = true;
					}
					else
					{
						LogAndEvent("Invalid metric name and alias.");
					}
				}
				else
				{
					// Check there is a name
					if (r.Name.Length > 0)
					{
						ProcessPointByName(r.Name, r);
						DataReceived = true;
					}
				}
			}
			if (DataReceived)
			{
				LogAndEvent("Flushing buffered updates.");
				this.FlushUpdates(); // Write to server - buffering this so only called once per device/time
			}
		}


		// Point data processing
		// By type/number
		public void ProcessPointByNumber(ulong pointnum, Payload.Types.Metric r)
        {
            // Found point is:
            PointSourceEntry FoundPoint = null;

            // Find point by WITS ID
            foreach (PointSourceEntry Entry in Points)
            {
                if (Entry.PointType == typeof(SparkplugBPointDg) )
                {
                    SparkplugBPointDg PointDg = (SparkplugBPointDg)Entry.DatabaseObject;
                    if (PointDg.Address == (int)pointnum )
                    {
                        FoundPoint = Entry;
                        break;
                    }
                }
				if (Entry.PointType == typeof(SparkplugBPointAg) )
				{
					SparkplugBPointAg PointAg = (SparkplugBPointAg)Entry.DatabaseObject;
					if (PointAg.Address == (int)pointnum )
					{
						FoundPoint = Entry;
						break;
					}
				}
				if (Entry.PointType == typeof(SparkplugBPointCi) )
				{
					SparkplugBPointCi PointCi = (SparkplugBPointCi)Entry.DatabaseObject;
					if (PointCi.Address == (int)pointnum)
					{
						FoundPoint = Entry;
						break;
					}
				}
				if (Entry.PointType == typeof(SparkplugBPointSt))
				{
					SparkplugBPointSt PointSt = (SparkplugBPointSt)Entry.DatabaseObject;
					if (PointSt.Address == (int)pointnum)
					{
						FoundPoint = Entry;
						break;
					}
				}
				if (Entry.PointType == typeof(SparkplugBPointTm))
				{
					SparkplugBPointTm PointTm = (SparkplugBPointTm)Entry.DatabaseObject;
					if (PointTm.Address == (int)pointnum)
					{
						FoundPoint = Entry;
						break;
					}
				}
			}

			if (FoundPoint != null)
            {
                ProcessPointData(FoundPoint, r);
            }
        }


        // Point data processing
        // By type/number
        public void ProcessPointByName(string pointname, Payload.Types.Metric r)
        {
            // Found point is:
            PointSourceEntry FoundPoint = null;

            // Find point by WITS ID
            foreach (PointSourceEntry Entry in Points)
            {
				if (Entry.PointType == typeof(SparkplugBPointDg))
				{
					SparkplugBPointDg PointDg = (SparkplugBPointDg)Entry.DatabaseObject;
					if (PointDg.SparkplugName == pointname)
					{
						FoundPoint = Entry;
						break;
					}
				}
				if (Entry.PointType == typeof(SparkplugBPointAg))
				{
					SparkplugBPointAg PointAg = (SparkplugBPointAg)Entry.DatabaseObject;
					if (PointAg.SparkplugName == pointname)
					{
						FoundPoint = Entry;
						break;
					}
				}
				if (Entry.PointType == typeof(SparkplugBPointCi))
				{
					SparkplugBPointCi PointCi = (SparkplugBPointCi)Entry.DatabaseObject;
					if (PointCi.SparkplugName == pointname)
					{
						FoundPoint = Entry;
						break;
					}
				}
				if (Entry.PointType == typeof(SparkplugBPointSt))
				{
					SparkplugBPointSt PointSt = (SparkplugBPointSt)Entry.DatabaseObject;
					if (PointSt.SparkplugName == pointname)
					{
						FoundPoint = Entry;
						break;
					}
				}
				if (Entry.PointType == typeof(SparkplugBPointTm))
				{
					SparkplugBPointTm PointTm = (SparkplugBPointTm)Entry.DatabaseObject;
					if (PointTm.SparkplugName == pointname)
					{
						FoundPoint = Entry;
						break;
					}
				}
			}

			if (FoundPoint != null)
            {
                ProcessPointData(FoundPoint, r);
            }
        }

        public void ProcessPointData(PointSourceEntry FoundPoint, Payload.Types.Metric r)
        {
			string ProcLogText = "";

			// Payload data includes:
			// name, alias - they were used to get here
			// timestamp - set to now if empty - an alternative is to use the payload timestamp?
			DateTime DataTime = DateTime.UtcNow;
			if (r.Timestamp > 0)
			{
				DataTime = util.UnixTimeStampMillisToDateTime(r.Timestamp);
			}
			// datatype - the numeric type code
			// is_historical - boolean. Can use this in future to skip processing of current data if this is True
			// is_transient - boolean.  Can use this in future to skip historic storage?
			// is_null - boolean. We may just ignore these values
			// value - one of uint32,  uint64, float, double, bool, string, (to ignore these: Bytes, DataSet, Template)

			if (r.IsNull)
            {
                LogAndEvent("No value - ignoring.");
                return;
            }

			ProcLogText += ("Point: " + FoundPoint.FullName + " Found a value.");

			// Quality
			PointSourceEntry.Quality quality = PointSourceEntry.Quality.Good;

            // Reason
            PointSourceEntry.Reason reason = PointSourceEntry.Reason.Report;

			LogAndEvent( ProcLogText);

			// TODO - handle state change, persistence etc.
			//if (FoundPoint.ValueChanged(qe, r.v[i])) // Only process if changed

			object value;
			switch (r.Datatype)
			{
				case 1:
				case 2:
				case 3:
				case 4:
				case 5:
				case 6:
				case 7:
					value = r.IntValue;
					break;
				case 8:
					value = r.LongValue;
					break;
				case 9:
					value = r.FloatValue;
					break;
				case 10:
					value = r.DoubleValue;
					break;
				case 11:
					value = r.BooleanValue;
					break;
				case 12:
				case 15:
					value = r.StringValue;
					break;
				case 13:
					value = r.LongValue;
					break;
				default:
					LogAndEvent("Unknown point data type");
					return;
			}


			FoundPoint.SetValue( DataTime,
								 quality,
								 reason,
								 value);
			//this.FlushUpdates(); // Write to server - defer this until all points have been processed - more efficient.
		}


		public override void OnControl(PointSourceEntry Point, object Value)
        {
			DrvSparkplugBBroker broker = (DrvSparkplugBBroker)Channel;
			byte QoS = broker.DBChannel.PubQoS;

			if (Point.PointType.Name == "SparkplugBPointDg") // Entry.PointType == typeof(SparkplugBPointDg)
			{
                ControlDigital(Point, Value, QoS);
            }
            else if (Point.PointType.Name == "SparkplugBPointAg") // Entry.PointType == typeof(SparkplugBPointAg)
			{
                ControlAnalogue(Point, Value, QoS);
            }
			else
			{
                throw new Exception("No handler found for " + Point.FullName);
            }
        }

        private void ControlDigital(PointSourceEntry entry, object val, byte QoS)
        {
            SparkplugBPointDg point = (SparkplugBPointDg)(entry.DatabaseObject);
			uint SPtype = (uint)((SparkplugBPointDg)entry.DatabaseObject).SPtype;
			string SPname = ((SparkplugBPointDg)entry.DatabaseObject).SparkplugName;

			SendControlMessage(val, SPtype, SPname, QoS);
		}

		private void ControlAnalogue(PointSourceEntry entry, object val, byte QoS)
		{
			SparkplugBPointAg point = (SparkplugBPointAg)(entry.DatabaseObject);
			uint SPtype = (uint)((SparkplugBPointAg)entry.DatabaseObject).SPtype;
			string SPname = ((SparkplugBPointAg)entry.DatabaseObject).SparkplugName;

			SendControlMessage(val, SPtype, SPname, QoS);
		}

		private void SendControlMessage(Object value, uint SPtype, string SPname, byte PubQoS)
		{
			// Build control message
			Payload ControlMessage = new Payload();

			// UTC time offset for Unix
			ControlMessage.Timestamp = (ulong)(DateTime.UtcNow - DateTime.Parse("01/Jan/1970")).Milliseconds;

			// Metric
			Payload.Types.Metric ControlMetric = new Payload.Types.Metric();
			ControlMetric.Name = SPname;
			ControlMetric.Timestamp = ControlMessage.Timestamp;
			// Get value based on type of control value
			switch (SPtype)
			{
				case 1:
				case 2:
				case 3:
				case 4:
				case 5:
				case 6:
				case 7:
					ControlMetric.IntValue = (uint)value;
					break;
				case 8:
					ControlMetric.LongValue = (uint)value;
					break;
				case 9:
					ControlMetric.FloatValue = (float)value;
					break;
				case 10:
					ControlMetric.DoubleValue = (double)value;
					break;
				case 11:
					int intvalue = (byte)value;
					ControlMetric.BooleanValue = (intvalue != 0);
					break;
				case 12:
				case 15:
					ControlMetric.StringValue = (string)value;
					break;
				case 13:
					ControlMetric.LongValue = (ulong)((DateTime)value - DateTime.Parse("01/Jan/1970")).Milliseconds;
					break;
				default:
					LogAndEvent("Unknown control type");
					return;
			}
			
			ControlMetric.Datatype = SPtype;
			ControlMessage.Metrics.Add(ControlMetric);

			LogAndEvent("Control message formed");

			// Control topic data
			string NodeDev = this.DBScanner.NodeDevice;
			string MessageType = "NCMD";
			if (this.DBScanner.DeviceId != "")
			{
				MessageType = "DCMD";
			}
			// Form topic - namespace/group_id/DCMD/edge_node_id/device_id
			DrvSparkplugBBroker broker = (DrvSparkplugBBroker)Channel;

			string TopicName = broker.DBChannel.NamespaceName + "/" +
								broker.DBChannel.GroupId + "/" +
								MessageType + "/" +
								NodeDev;
			byte[] bytes;

			// Add sequence number
			ControlMessage.Seq = (ulong)tx_seq;

			bytes = ControlMessage.ToByteArray();
			broker.DoPublish( TopicName , bytes, PubQoS, false);
			// QoS=1, no retain

			tx_seq++;
		}
	}

	class ConnectivityOptions
	{
		public string hostname;
		public UInt16 portnumber;
		public string username;
		public string password;
		public byte version;
		public string clientId;
		public byte security;
		public string caCertFile;
		public string clientCertFile;
		public byte clientCertFormat;
		public string clientCertPassword;
		public bool CleanConnect;
		public byte SubQoS;
		public byte PubQoS;
	}
}
