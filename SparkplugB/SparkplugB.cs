using System;
using System.Collections.Generic;
using System.Text;
using System.Runtime.Remoting.Messaging;
using System.IO;
using System.Web.Script.Serialization;

using ClearSCADA.DBObjFramework;
using ClearSCADA.DriverFramework;

using Google.Protobuf;
using Org.Eclipse.Tahu.Protobuf;


[assembly:Category("SparkplugB")]
[assembly:Description("SparkplugB Driver")]
[assembly:DriverTask("DriverSparkplugB.exe")]

[System.ComponentModel.RunInstaller(true)]
public class CSInstaller :  DriverInstaller
{
}

namespace SparkplugB
{
    public class CSharpModule : DBModule
    {
    }

    [Table("SparkplugB Broker", "SparkplugB")]
	[EventCategory("SparkplugBBroker", "SparkplugB Broker", OPCProperty.Base + 0)]
	[EventCategory("SparkplugBBrokerDbg", "SparkplugB Broker Debug", OPCProperty.Base + 81)]
	public class SparkplugBBroker : Channel
    {
        [AlarmCondition("SparkplugChannelBrokerAlarm", "SparkplugBBroker", 0x03505041)]
        [AlarmSubCondition("SparkplugChannelCommError")]
        [AlarmSubCondition("SparkplugChannelNewDevice")]
        [AlarmSubCondition("SparkplugChannelDeviceConfig")]
        public Alarm SparkplugChannelAlarm;

        [Label("In Service", 1, 1)]
        [ConfigField("In Service", "Controls whether the channel is active.", 1, 2, 0x0350501B)]
        public override Boolean InService
        {
            get
            {
                return base.InService;
            }
            set
            {
                base.InService = value;
            }
        }

		[Label("Enhanced Events", 1, 3)]
		[ConfigField("Enhanced Events", "Controls whether debug messages are sent to the event log. (Do not use long-term).", 1, 4, OPCProperty.Base + 83)]
		public Boolean EnhancedEvents = false;

		[Label("Severity", 2, 1)]
        [ConfigField("Severity", "Severity", 2, 2, 0x0350501C)]
        public override ushort Severity
        {
            get
            {
                return base.Severity;
            }
            set
            {
                base.Severity = value;
            }
        }

		// Need to modify the below if there is C
		[Label("Namespace", 2, 3)]
		[ConfigField("Namespace",
						"The namespace # of the Sparkplug protocol.",
						2, 4, OPCProperty.Base + 110)]
		[Enum(new String[] { "spBv1.0" })]
		public Byte Namespace = 0;

		[ConfigField("NamespaceName",
						"The namespace name of the Sparkplug protocol.",
						2, 4, OPCProperty.Base + 111,Flags = FormFlags.Hidden, ReadOnly = true)]
		public string NamespaceName = "spBv1.0";

		[Label("Area Of Interest", 3, 1, AreaOfInterest = true)]
        [ConfigField("Area Of Interest", "Reference to the Area Of Interest in which alarms & events on this object occur.", 3, 2, 0x03600D00)]
        public override AOIReference AreaOfInterestIdBase
        {
            get
            {
                return base.AreaOfInterestIdBase;
            }
            set
            {
                base.AreaOfInterestIdBase = value;
            }
        }

        [ConfigField("Area Of Interest", "Name of the Area Of Interest in which alarms & events on this object occur.", 3, 4, 0x03600D04, ReadOnly = true, Length = 48, Flags = FormFlags.Hidden)]
        public String AreaOfInterestBase
        {
            get { return AreaOfInterestIdBase.Name; }
        }

        [Label("Broker Host", 4, 1)]
        [ConfigField("BrokerHost",
                     "The IP address or network name of the MQTT broker",
                     4, 2, OPCProperty.Base + 1, Length = 80)]
        public string BrokerHost;

        [Label("Broker Port", 4, 3)]
        [ConfigField("BrokerPort",
                     "The IP port of the MQTT broker",
                     4, 4, OPCProperty.Base + 53)]
        public UInt16 BrokerPort = 1883; // Default. SSL Default is 8883

        [Label("Username", 5, 1)]
        [ConfigField("Username",
                     "The user name configured for login to the MQTT broker. Leave blank if unused.",
                     5, 2, OPCProperty.Base + 54, Length = 80)]
        public string Username;

        [Label("Password", 5, 3)]
        [ConfigField("Password",
                     "The password configured for login to the MQTT broker.",
                     5, 4, OPCProperty.Base + 55, Length = 300, Flags = FormFlags.Password)]
        public string Password;

        [Label("Version", 6, 1)]
        [ConfigField("MQTTVersion",
                        "The version of the MQTT connection.",
                        6, 2, OPCProperty.Base + 10)]
        [Enum(new String[] { "3.1", "3.1.1" })]
        public Byte MQTTVersion = 1;

        [Label("Client Id", 6, 3)]
        [ConfigField("ClientId",
                     "The client identification string for connection to the MQTT broker.",
                     6, 4, OPCProperty.Base + 11, Length = 80)]
        public string ClientId = Guid.NewGuid().ToString();

        [Label("Security", 7, 1)]
        [ConfigField("Security",
                        "The type of encryption to be used for the MQTT connection.",
                        7, 2, OPCProperty.Base + 12)]
        [Enum(new String[] { "None", "SSL v3", "TLS v1.0", "TLS v1.1", "TLS v1.2" })]
		public Byte Security = 0;

		[Label("Group Id", 7, 3)]
		[ConfigField("SourceId",
					 "The Sparkplug Group name.",
					 7, 4, OPCProperty.Base + 94, Length = 80)]
		public string GroupId = "";

		[Label("CA Certificate Filename", 8, 1)]
        [ConfigField("caCertFile",
                     "CA X509 Certificate File Name.",
                     8, 2, OPCProperty.Base + 13, Length = 120)]
        public string caCertFile;

        [Label("Client Certificate Filename", 8, 3)]
        [ConfigField("clientCertFile",
                     "Client X509 Certificate File Name.",
                     8, 4, OPCProperty.Base + 14, Length = 120)]
        public string clientCertFile;

        [Label("X509 Certificates in DER format.", 8, 5)]

		[Label("Client Cert Format", 9, 3)]
		[ConfigField("ClientCertFormat",
						"The certificate format for the client certificate.",
						9, 4, OPCProperty.Base + 201)]
		[Enum(new String[] { "DER", "PFX / PKCS12" })]
		public Byte ClientCertFormat = 0;

		[Label("Client Certificate Password", 10, 3)]
		[ConfigField("ClientCertPassword",
					 "Password for the client certificate.",
					 10, 4, OPCProperty.Base + 202, Length = 80, Flags = FormFlags.Password)]
		[Enable("ClientCertFormat = 1")]
		public string ClientCertPassword = "";

		[Label("SCADA Host Id", 9, 1)]
		[ConfigField("SCADAHostId",
					 "SCADA Host identification for online/offline state management by Sparkplug.",
					 9, 2, OPCProperty.Base + 15, Length = 80)]
		public string SCADAHostId = "HostA";

		// AWS doesn't support retain
		[Label("Disable retain flag/force clean connection", 11, 3)]
		[ConfigField("CleanConnect",
					 "Do not set retain flag on connect, and each new connection to broker is marked as clean.",
					 11, 4, OPCProperty.Base + 203)]
		public bool CleanConnect;

		// AWS does not support 2, will insta-disconnect if you try
		[Label("Subscribe QoS", 12, 3)]
		[ConfigField("SubQoS",
				"The QoS to use for subscribing.",
				12, 4, OPCProperty.Base + 204)]
		[Enum(new String[] { "0", "1", "2" })]
		public Byte SubQoS = 2;

		[Label("Publish QoS", 13, 3)]
		[ConfigField("PubQoS",
				"The QoS to use for publishing.",
				13, 4, OPCProperty.Base + 205)]
		[Enum(new String[] { "0", "1", "2" })]
		public Byte PubQoS = 1;

		#region AutoConfig
		[Label("Automatic Configure", 10, 1)]
        [ConfigField("AutoConfig",
                     "Automatically configure unknown devices.",
                     10, 2, OPCProperty.Base + 2)]
        public bool AutoConfig;

		[Label("Server Port", 11, 1)]
        [ConfigField("ServerPort",
                     "Port for client to make configuration changes.",
                     11, 2, OPCProperty.Base + 34)]
        public UInt32 ServerPort = 5481;

        [Label("Configuration User Name", 12, 1)]
        [ConfigField("ConfigUserName",
                     "Geo SCADA User needed to log in and make configuration change.",
                     12, 2, OPCProperty.Base + 38, Length = 32)]
        public string ConfigUserName;

        [Label("Configuration Password", 13, 1)]
        [ConfigField("ConfigPass",
                     "Get SCADA User credential to log in and make configuration change.",
                     13, 2, OPCProperty.Base + 35, Length = 40, Flags = FormFlags.Password)]
        public String ConfigPass;

        [Label("Create Devices or Instances in Group", 14, 1)]
        [ConfigField("ConfigGroupId",
                     "Folder in which new sites are created automatically (you may wish this to be the Group Name).",
                     14, 2, OPCProperty.Base + 36, RefTables = "CGroup")]
        public Reference<DBObject> ConfigGroupId;

		[Label("Node Template", 15, 1)]
		[ConfigField("TemplateId",
			 "Node Template reference (optional). New configurations will use this template",
			 15, 2, OPCProperty.Base + 37, RefTables = "CTemplate")]
		public Reference<DBObject> TemplateId;

		// This will contain templates with the same name as the Devices
		[Label("Device Templates", 16, 1)]
        [ConfigField("DeviceTemplatesId",
                     "Group in which Device templates are found (optional).",
                     16, 2, OPCProperty.Base + 73, RefTables = "CGroup")]
        public Reference<DBObject> DeviceTemplatesId;
        #endregion

        #region DataFields
        // To be maintained as a database item. This is an array, so can be understood by ViewX
        [DataField("ConfigBuf", "Buffer of pending device configuration", OPCProperty.Base + 9)]
        public string[] ConfigBuf = new string[0];

        [DataField("PendingCount", "Count of pending devices for configuration", OPCProperty.Base + 41, ReadOnly =true, ViewInfoTitle = "Pending Devices for Configuration")]
        public int PendingCount
        {
            get
            {
                return ConfigBuf.Length;
            }
        }

        [DataField("LastConfigId", "Latest unconfigured device Node/ENodeId", OPCProperty.Base + 42, ReadOnly = true, ViewInfoTitle = "Latest Device for Configuration")]
        public string LastConfigId
        {
            get
            {
                if (ConfigBuf.Length > 0)
                {
                    return ConfigBuf[0];
                }
                else
                {
                    return "";
                }
            }
        }
        #endregion

        public override void OnValidateConfig(MessageInfo Errors)
        {
			if (GroupId == "")
			{
				Errors.Add(this, "GroupId", "GroupId is blank.");
			}
            base.OnValidateConfig(Errors);
        }

        public override void OnReceive(uint Type, object Data, ref object Reply)
        {

            if (Type == OPCProperty.SendRecClearChannelAlarm)
            {
                if (SparkplugChannelAlarm.ActiveSubCondition == "SparkplugChannelCommError")
                {
                    SparkplugChannelAlarm.Clear();
                    SetDataModified(true);
                }
            }
            else if (Type == OPCProperty.SendRecRaiseChannelAlarm)
            {
                // If not already active subcondition AND uncleared.
                if ((SparkplugChannelAlarm.ActiveSubCondition != "SparkplugChannelCommError") && (SparkplugChannelAlarm.State != 4) && (SparkplugChannelAlarm.State != 2))
                {
                    SparkplugChannelAlarm.Raise("SparkplugChannelCommError", "SparkplugB Error: Broker is Offline", Severity, true);
                    SetDataModified(true);
                }
            }
            else if (Type == OPCProperty.SendRecRequestConfiguration)
            {
                string NodeDeviceId = (string)Data;
                LogSystemEvent("SparkplugBBroker", Severity, "Request for configuration initiation from: " + NodeDeviceId);
                SparkplugChannelAlarm.Raise("SparkplugChannelNewDevice", "SparkplugB New Device Connected: " + NodeDeviceId, Severity, true);
				// Raises an alarm which 'tells' user to check and then request config as a method action on the broker ConfigurePending
            }
            else if (Type == OPCProperty.SendRecUpdateConfigQueue)
            {
				// Commented the item below out/removed from the above action. All q items are refreshed by driver in one action whenever list changes.
				// Save the device NodeDeviceId into a list so a user can ask for this to be configured - or rejected?
				//Array.Resize(ref deviceBuf, deviceBuf.Length + 1);
				//deviceBuf[deviceBuf.Length - 1] = i.uuid;

				// Refresh the pending queue
				ConfigBuf = (string[])Data;
            }
            else if (Type == OPCProperty.SendRecReportConfigError)
            {
                String Err = (String)Data;
                SparkplugChannelAlarm.Raise("SparkplugChannelDeviceConfig", "SparkplugB Error configuring device: " + Err, Alarm.AlarmType.Fleeting, Severity, true);
            }
			else if (Type == OPCProperty.SendRecLogBrokerEventText)
			{
				String Message = (String)Data;
				LogSystemEvent("SparkplugBBroker", Severity, Message);
			}
			else
			{
                base.OnReceive(Type, Data, ref Reply);
            }
        }

		[Method("Configure Pending Devices", "Configure all devices which are pending", OPCProperty.Base + 32)]
        public void ConfigurePending()
        {
            // Queue a config request for each device, then the driver will delete the queue contents
            for (int i=0; i < ConfigBuf.Length; i++ )
            {
                object[] ArgObject = new Object[1];
                ArgObject[0] = ConfigBuf[i];
                DriverAction(OPCProperty.DriverActionInitiateConfig, ArgObject, "Initiate configuration of new Device: " + ConfigBuf[i]);
            }
        }

	}

	[Table("SparkplugB Node Device", "SparkplugB")]
    [EventCategory("SparkplugBND", "SparkplugB ND", OPCProperty.Base + 3)]
	[EventCategory("SparkplugBNDDbg", "SparkplugB ND Debug", OPCProperty.Base + 82)]
	public class SparkplugBND : Scanner
    {
        [AlarmCondition("SparkplugDeviceScannerAlarm", "SparkplugBND", 0x0350532F)]
		[AlarmSubCondition("SparkplugCommSeq")]
		[AlarmSubCondition("SparkplugCommLWT")]
        public Alarm SparkplugScannerAlarm;

        [AlarmCondition("SparkplugDeviceConfigAlarm", "SparkplugBND", OPCProperty.Base + 19)]
        [AlarmSubCondition("SparkplugNodeIdError")]
        [AlarmSubCondition("SparkplugConfigVerError")]
        public Alarm SparkplugConfigAlarm;

        [Label("Enabled", 1, 1)]
        [ConfigField("In Service", 
                     "Controls whether broker connection is active.",
                     1, 2, 0x350501B, DefaultOverride =true)]
        public override Boolean InService 
        {
            get 
            { 
                return base.InService;
            }
            set 
            { 
                base.InService = value;
            }
        }

        [Label("Severity", 2, 1)]
        [ConfigField("Severity", "Severity", 2, 2, 0x0350501C)]
        public override ushort Severity
        {
            get
            {
                return base.Severity;
            }
            set
            {
                base.Severity = value;
            }
		}

		[Label("Scan Interval", 1, 3)] 
		[ConfigField("ScanRate", "Status Check Interval.", 1, 4, 0x03505045)] 
		[Interval(IntervalType.Seconds)]
		public UInt32 NormalScanRate = 30;

		[Label("Scan Offset", 2, 3)]
		[ConfigField("ScanOffset", "Check offset", 2, 4, 0x0350504D, Length = 32)]
		public String NormalScanOffset = "M";

        [Label("Broker", 3, 1)]
        [ConfigField("Channel", "Broker Reference", 3, 2, 0x03505041)]
        public Reference<SparkplugBBroker> ChannelId;

        [Label("Edge Node Id", 4, 1)]
        [ConfigField("ENodeId",
                     "The Edge Node Identification.",
                     4, 2, OPCProperty.Base + 4, Length = 80, AlwaysOverride =true)]
        public string ENodeId;

		[Label("Device Id", 4, 3)]
		[ConfigField("DeviceId", "The Device Identification. Blank if this is a Node.", 4, 4, OPCProperty.Base + 18, DefaultOverride = true, Length = 20)]
		public string DeviceId;

		[Label("Parent Node", 5, 1)]
		[ConfigField("ParentNodeId",
					 "The Parent Edge Node object, if this is a child Device.",
					 5, 2, OPCProperty.Base + 5, DefaultOverride = true, RefTables = "SparkplugBND")]
		public Reference<DBObject> ParentNodeId;

		[Label("Area of Interest", 6, 1, AreaOfInterest = true)]
        [ConfigField("AOI Ref", "A reference to an AOI.", 6, 2, 0x0465700E)]
        public AOIReference AOIRef;

        [ConfigField("AOI Name", "A reference to an AOI.", 6, 3, 0x0465700F,
                     ReadOnly = true, Length = 48, Flags = FormFlags.Hidden)]
        public String AOIName
        {
            get { return AOIRef.Name; }
        }

		// Data Fields here
		[DataField("Node Device Name",
				   "Node Id / Device Id as one string",
				   OPCProperty.Base + 8)]
		public String NodeDevice
		{
			get
			{
				if (DeviceId == "")
				{
					return ENodeId;
				}
				return ENodeId + "/" + DeviceId;
			}
		}

		[DataField("Last Error",
                   "The text of the last error.",
                   OPCProperty.Base + 7, ViewInfoTitle = "Last Error")]
        public String ErrMessage;

		[DataField("Time of Last Birth", "Timestamp in last birth certificate", OPCProperty.Base + 17,
					ViewInfoTitle = "Time of Birth")]
		public DateTime LastBirth;

		[DataField("Sequence of Last Birth", "Sequence number in last birth certificate", OPCProperty.Base + 21,
					ViewInfoTitle = "Seq of Birth")]
		public Int16 LastBirthSeq;

		[DataField("Time of Last Death", "Time when Death certificate was last received", OPCProperty.Base + 23,
					ViewInfoTitle = "Time of Death")]
		public DateTime LastDeath;

		[DataField("Sequence of Last Death", "Sequence number in last death certificate", OPCProperty.Base + 24,
					ViewInfoTitle = "Seq of Death")]
		public Int16 LastDeathSeq;

		[DataField("Config Required", "Configuration required flag", OPCProperty.Base + 20,
                    ViewInfoTitle = "Configuration Required")]
        public bool ConfReq = false; // Unconfigured

        [DataField("Active State", "Active State flag", OPCProperty.Base + 22,
                    ViewInfoTitle = "Active State")]
        public bool ActiveState = true; 

		[ConfigField("ConfigText", "The binary of the Device Config", 1, 1, OPCProperty.Base + 60, Flags = FormFlags.Hidden)]
		public string ConfigText = ""; // As Base64 string.

		[ConfigField("ConfigChecksum", "Checksum/hash of the Device Config", 1, 1, OPCProperty.Base + 88, Flags = FormFlags.Hidden)]
		public string ConfigChecksum = "";

		// Save Config from structure
		public void SetConfig(Payload p)
		{
			ConfigText = Payload2Base64(p);
			UpdateConfigChecksum();
		}

		// Load Config from a file on server's disk
		// (Used for test/debug only)
		public bool LoadConfig(string FileName)
		{
			// Get from disk
			if (File.Exists(FileName))
			{
				var chardata = File.ReadAllBytes(FileName);
				ConfigText = Convert.ToBase64String(chardata);
				UpdateConfigChecksum();
				return true;
			}
			return false;
		}

		public string Payload2Base64( Payload p)
		{
			// Serialize, convert to Base64 and write
			byte[] bytes;
			using (MemoryStream stream = new MemoryStream())
			{
				p.WriteTo(stream);
				bytes = stream.ToArray();
			}
			return Convert.ToBase64String(bytes);
		}

		public void UpdateConfigChecksum()
		{
			ConfigChecksum = util.GetHashString(ConfigText);
		}

		public bool ValidateConfigChecksum(string NewConfigText)
		{
			return (ConfigChecksum == util.GetHashString(NewConfigText));
		}

		public override void OnValidateConfig(MessageInfo Errors)
        {
			// Don't validate templated nodes/devices
			if (!IsTemplate())
			{
				// ToDo - e.g. check ENodeId and DeviceId do not contain /, + or #
				if (ENodeId == "")
				{
					Errors.Add(this, "ENodeId", "Edge Node name must not be blank.");
				}
			}
            base.OnValidateConfig(Errors);
        }

		public override void OnReceive(uint Type, object Data, ref object Reply)
        {
            // Clear scanner alarm
            if (Type == OPCProperty.SendRecClearScannerAlarm)
            {
                SparkplugScannerAlarm.Clear();
                SetDataModified(true);
            }
            // Set scanner alarm
            else if (Type == OPCProperty.SendRecRaiseScannerAlarm)
            {
                SparkplugScannerAlarm.Raise("SparkplugCommLWT", "SparkplugCommError: Offline Last Will Received", Severity, true);
                SetDataModified(true);
            }
			else if (Type == OPCProperty.SendRecProcessLWT)
			{
				Int16 SeqNumber = (Int16)Data;
				LogSystemEvent("SparkplugBND", Severity, "Received Last Will and Testament. Seq: " + SeqNumber.ToString());

				//Sequence numbers
				LastDeathSeq = SeqNumber;
				LastDeath = DateTime.UtcNow;
				// Check sequence is valid - only if this is a node and not a device
				if (DeviceId == "")
				{
					// Check done for both Node and Device. Device birth/death sequences should always be zero, so should not raise an alarm?
					if (LastDeathSeq != LastBirthSeq)
					{
						// Error, raise alarm
						SparkplugScannerAlarm.Raise("SparkplugCommSeq", "SparkplugCommError: Invalid Birth/Death Sequence", Severity, true);
					}
				}
				//TODO Set quality of node points to stale, but not? child device points to stale?

				SetDataModified(true);
			}
			// Recieve Birth data (configuration) for known device
			else if (Type == OPCProperty.SendRecProcessBCStatus)
            {
				// Data is a byte array of a payload
				byte[] DataBytes = (byte [])(Data);
				Payload bc = Payload.Parser.ParseFrom(DataBytes);

				System.DateTime Timestamp = util.UnixTimeStampMillisToDateTime(bc.Timestamp);
				LogSystemEvent("SparkplugBND", Severity, "Received Birth Certificate. Time: " + Timestamp.ToString());

				// Record birth data
                LastBirth = Timestamp;
				LastBirthSeq = (Int16) bc.Seq;

				// Could use this field to set out of service?
				if (!ActiveState)
				{
					LogSystemEvent("SparkplugBND", Severity, "Device is inactive.");
				}

				// Scanner good, Clear alarm
				SparkplugScannerAlarm.Clear();
				SetDataModified(true);

				// Receive configuration for known device

				// Flag to assume we write a new config, unless can't
				bool writeNewConfig;

                LogSystemEvent("SparkplugBND", Severity, "Received Device Configuration." );
				
				// Is the config checksum different? Create a Payload with only the Metrics
				Payload MetricsOnly = new Payload();
				foreach ( Payload.Types.Metric m in bc.Metrics)
				{
					MetricsOnly.Metrics.Add(m);
				}
				string ConfigReceived = Payload2Base64(MetricsOnly);

				if (ConfigReceived == ConfigText)
				{
					LogSystemEvent("SparkplugBND", Severity, "Received configuration is the same as the saved version.");
					writeNewConfig = false;
				}
				else
				{
					// New configuration received - process this !
					LogSystemEvent("SparkplugBND", Severity, "Received configuration for updating an existing device.");
					writeNewConfig = true;
				}

				if (writeNewConfig)
				{
					LogSystemEvent("SparkplugBND", Severity, "Received new configuration - writing to Field Device.");

					SetConfig(MetricsOnly);

					// Need to receive configuration
					LogSystemEvent("SparkplugBND", Severity, "Configuration is requested.");
					ConfReq = true;
				}
				// Pass back the reply status as needing config or not.
				Reply = writeNewConfig ? "Yes" : "No";
			}
			else if (Type == OPCProperty.SendRecFDProtocolError)
            {
                // Error interpreting a JSON message - Log Event (may upgrade to alarm later?)
                LogSystemEvent("SparkplugBND", Severity, (string)Data);
            }
			else if (Type == OPCProperty.SendRecLogFDEventText)
			{
				// General debug message raised as event
				String Message = (String)Data;
				LogSystemEvent("SparkplugBND", Severity, Message);
			}
			else if (Type == OPCProperty.SendRecClearConfReq)
			{
				ConfReq = false;
				LogSystemEvent("SparkplugBND", Severity, "Clear Configuration Required Flag.");
			}
			else
				base.OnReceive(Type, Data, ref Reply);
        }

		[Method("Reset Config Version", "Reset configuration data on this device.", OPCProperty.Base + 86)]
		public void ResetConfig()
		{
			ConfigText = "";
			ConfigChecksum = "";
			object[] ArgObject = new Object[1];
			ArgObject[0] = ""; // No parameters
			DriverAction(OPCProperty.DriverActionResetConfigFD, ArgObject, "Reset Configuration Data");
		}

	}

    [Table("SparkplugB Digital", "SparkplugB")]
	public class SparkplugBPointDg : DigitalPoint
	{
        // Derived configuration field
        // This flag is stored and saved in the base class
        // To configure it it needs to be exposed by an override
        [Label("In Service", 1, 1)]
        [ConfigField("In Service",
        "Controls whether the point is active.",
        1, 2, 0x0350501B)]
        public override bool InService
        {
            get
            {
                return base.InService;
            }
            set
            {
                base.InService = value;
            }
        }
        
        [Label("Device", 2, 1)]
        [ConfigField("Scanner", "Scanner", 2, 2, 0x0350532F)]
        public new Reference<SparkplugBND> ScannerId
		{
			get
			{
                return new Reference<SparkplugBND>(base.ScannerId);
			}
			set
			{
				base.ScannerId = new Reference<Scanner>(value);
			}
		}

        [Label("Sparkplug Name", 2, 3)]
        [ConfigField("SparkplugName", "Sparkplug point name.", 2, 4, OPCProperty.Base + 74, Length = 40, AlwaysOverride = true)]
        public string SparkplugName;

        // Common per type
		[Label("Address Alias", 3, 1)]
		[ConfigField("Address",
					 "The numeric address alias of the point",
					 3, 2, OPCProperty.Base + 6)]
		public int Address;

		// Read-only data field containing a type Name
		[Label("Sparkplug Datatype", 3, 3)]
		[ConfigField("SPtype", "Point Type Number", 3, 4, OPCProperty.Base + 72, DefaultOverride = true)]
		public int SPtype;

        [Aggregate("Enabled", "Control", 0x03600000, "CCtrlDigital")]
		public Aggregate Control;

        // Group of configuration fields
        // Normally an attribute on the first config field of the group
        [Label("Historic Data Filter", 4, 1, Width=2, Height=4)]
        // Historic filter is indexed property
        // Expose each item as a single tick box
        [Label("Significant Change", 5, 1)]
        [ConfigField("Report Historic Filter",
                    "Controls whether Significant Value Changes are logged historically",
                    5, 2, 0x3506913 )]
        public bool HistoricFilterValue
        {
            get
            {
                return base.get_HistoricFilter(PointSourceEntry.Reason.ValueChange);
            }
            set
            {
                base.set_HistoricFilter(PointSourceEntry.Reason.ValueChange, value);
            }
        }
        [Label("State Change", 6, 1)]
        [ConfigField("Report Historic Filter",
                    "Controls whether State Changes are logged historically",
                    6, 2, 0x3506914 )]
        public bool HistoricFilterState
        {
            get
            {
                return base.get_HistoricFilter(PointSourceEntry.Reason.StateChange);
            }
            set
            {
                base.set_HistoricFilter(PointSourceEntry.Reason.StateChange, value);
            }
        }
        [Label("Report", 7, 1)]
        [ConfigField("Report Historic Filter",
                "Controls whether Timed Report values are logged historically",
                7, 2, 0x3506915 )]
        public bool HistoricFilterReport
        {
            get
            {
                return base.get_HistoricFilter(PointSourceEntry.Reason.Report);
            }
            set
            {
                base.set_HistoricFilter(PointSourceEntry.Reason.Report, value);
            }
        }
        [Label("End of Period Report", 8, 1)]
        [ConfigField("EOP Historic Filter",
                "Controls whether End of Period values are logged historically",
                8, 2, 0x3506916 )]
        public bool HistoricFilterEOP
        {
            get
            {
                return base.get_HistoricFilter(PointSourceEntry.Reason.EndofPeriod);
            }
            set
            {
                base.set_HistoricFilter(PointSourceEntry.Reason.EndofPeriod, value);
            }
        }


		public override void OnConfigChange(ConfigEvent Event, MessageInfo Errors, DBObject OldObject)
		{
			base.OnConfigChange(Event, Errors, OldObject);
		}

		public override void OnValidateConfig(MessageInfo Errors)
		{
			//ToDo Check topics not empty

			//Errors.Add("A config error messsage.");
			base.OnValidateConfig(Errors);
		}
	}


	[Table("SparkplugB Analogue", "SparkplugB")]
	public class SparkplugBPointAg : AnaloguePoint
	{
        // Derived configuration field
        // This flag is stored and saved in the base class
        // To configure it it needs to be exposed by an override
        [Label("In Service", 1, 1)]
        [ConfigField("In Service",
        "Controls whether the point is active.",
        1, 2, 0x0350501B)]
        public override bool InService
        {
            get
            {
                return base.InService;
            }
            set
            {
                base.InService = value;
            }
        }

        [Label("Device", 2, 1)]
        [ConfigField("Scanner", "Scanner", 2, 2, 0x0350532F)]
        public new Reference<SparkplugBND> ScannerId
        {
            get
            {
                return new Reference<SparkplugBND>(base.ScannerId);
            }
            set
            {
                base.ScannerId = new Reference<Scanner>(value);
            }
        }

        [Label("Sparkplug Name", 2, 3)]
        [ConfigField("SparkplugName", "Sparkplug point name.", 2, 4, OPCProperty.Base + 74, Length = 40, AlwaysOverride = true)]
        public string SparkplugName;

        // Common per type
        [Label("Address Alias", 3, 1)]
		[ConfigField("Address",
					 "The numeric address alias of the point",
					 3, 2, OPCProperty.Base + 6)]
		public int Address;

		// Read-only data field containing a type Name
		[Label("Sparkplug Datatype", 3, 3)]
		[ConfigField("SPtype", "Point Type Number", 3, 4, OPCProperty.Base + 72, DefaultOverride = true)]
		public int SPtype;

		[Aggregate("Enabled", "Control", 0x03600000, "CCtrlAlg")]
		public Aggregate Control;

        // Group of configuration fields
        // Normally an attribute on the first config field of the group
        [Label("Historic Data Filter", 4, 1, Width = 2, Height = 4)]
        // Historic filter is indexed property
        // Expose each item as a single tick box
        [Label("Significant Change", 5, 1)]
        [ConfigField("Report Historic Filter",
                    "Controls whether Significant Value Changes are logged historically",
                    5, 2, 0x3506913)]
        public bool HistoricFilterValue
        {
            get
            {
                return base.get_HistoricFilter(PointSourceEntry.Reason.ValueChange);
            }
            set
            {
                base.set_HistoricFilter(PointSourceEntry.Reason.ValueChange, value);
            }
        }
        [Label("State Change", 6, 1)]
        [ConfigField("Report Historic Filter",
                    "Controls whether State Changes are logged historically",
                    6, 2, 0x3506914)]
        public bool HistoricFilterState
        {
            get
            {
                return base.get_HistoricFilter(PointSourceEntry.Reason.StateChange);
            }
            set
            {
                base.set_HistoricFilter(PointSourceEntry.Reason.StateChange, value);
            }
        }
        [Label("Report", 7, 1)]
        [ConfigField("Report Historic Filter",
                "Controls whether Timed Report values are logged historically",
                7, 2, 0x3506915)]
        public bool HistoricFilterReport
        {
            get
            {
                return base.get_HistoricFilter(PointSourceEntry.Reason.Report);
            }
            set
            {
                base.set_HistoricFilter(PointSourceEntry.Reason.Report, value);
            }
        }
        [Label("End of Period Report", 8, 1)]
        [ConfigField("EOP Historic Filter",
                "Controls whether End of Period values are logged historically",
                8, 2, 0x3506916)]
        public bool HistoricFilterEOP
        {
            get
            {
                return base.get_HistoricFilter(PointSourceEntry.Reason.EndofPeriod);
            }
            set
            {
                base.set_HistoricFilter(PointSourceEntry.Reason.EndofPeriod, value);
            }
        }

		public override void OnConfigChange(ConfigEvent Event, MessageInfo Errors, DBObject OldObject)
		{
			base.OnConfigChange(Event, Errors, OldObject);
		}

		public override void OnValidateConfig(MessageInfo Errors)
		{
			//ToDo Check topics not empty

			//Errors.Add("A config error messsage.");
			base.OnValidateConfig(Errors);
		}
	}

	[Table("SparkplugB String", "SparkplugB")]
	public class SparkplugBPointSt : StringPoint
	{
		// Derived configuration field
		// This flag is stored and saved in the base class
		// To configure it it needs to be exposed by an override
		[Label("In Service", 1, 1)]
		[ConfigField("In Service",
		"Controls whether the point is active.",
		1, 2, 0x0350501B)]
		public override bool InService
		{
			get
			{
				return base.InService;
			}
			set
			{
				base.InService = value;
			}
		}

		[Label("Device", 2, 1)]
		[ConfigField("Scanner", "Scanner", 2, 2, 0x0350532F)]
		public new Reference<SparkplugBND> ScannerId
		{
			get
			{
				return new Reference<SparkplugBND>(base.ScannerId);
			}
			set
			{
				base.ScannerId = new Reference<Scanner>(value);
			}
		}

		[Label("Sparkplug Name", 2, 3)]
		[ConfigField("SparkplugName", "Sparkplug point name.", 2, 4, OPCProperty.Base + 74, Length = 40, AlwaysOverride = true)]
		public string SparkplugName;

		// Common per type
		[Label("Address Alias", 3, 1)]
		[ConfigField("Address",
					 "The numeric address alias of the point",
					 3, 2, OPCProperty.Base + 6)]
		public int Address;

		// Read-only data field containing a type Name
		[Label("Sparkplug Datatype", 3, 3)]
		[ConfigField("SPtype", "Point Type Number", 3, 4, OPCProperty.Base + 72, DefaultOverride = true)]
		public int SPtype;

		[Aggregate("Enabled", "Control", 0x03600000, "CCtrlString")]
		public Aggregate Control;

		// Group of configuration fields
		// Normally an attribute on the first config field of the group
		[Label("Historic Data Filter", 4, 1, Width = 2, Height = 4)]
		// Historic filter is indexed property
		// Expose each item as a single tick box
		[Label("Significant Change", 5, 1)]
		[ConfigField("Report Historic Filter",
					"Controls whether Significant Value Changes are logged historically",
					5, 2, 0x3506913)]
		public bool HistoricFilterValue
		{
			get
			{
				return base.get_HistoricFilter(PointSourceEntry.Reason.ValueChange);
			}
			set
			{
				base.set_HistoricFilter(PointSourceEntry.Reason.ValueChange, value);
			}
		}
		[Label("State Change", 6, 1)]
		[ConfigField("Report Historic Filter",
					"Controls whether State Changes are logged historically",
					6, 2, 0x3506914)]
		public bool HistoricFilterState
		{
			get
			{
				return base.get_HistoricFilter(PointSourceEntry.Reason.StateChange);
			}
			set
			{
				base.set_HistoricFilter(PointSourceEntry.Reason.StateChange, value);
			}
		}
		[Label("Report", 7, 1)]
		[ConfigField("Report Historic Filter",
				"Controls whether Timed Report values are logged historically",
				7, 2, 0x3506915)]
		public bool HistoricFilterReport
		{
			get
			{
				return base.get_HistoricFilter(PointSourceEntry.Reason.Report);
			}
			set
			{
				base.set_HistoricFilter(PointSourceEntry.Reason.Report, value);
			}
		}
		[Label("End of Period Report", 8, 1)]
		[ConfigField("EOP Historic Filter",
				"Controls whether End of Period values are logged historically",
				8, 2, 0x3506916)]
		public bool HistoricFilterEOP
		{
			get
			{
				return base.get_HistoricFilter(PointSourceEntry.Reason.EndofPeriod);
			}
			set
			{
				base.set_HistoricFilter(PointSourceEntry.Reason.EndofPeriod, value);
			}
		}

		public override void OnConfigChange(ConfigEvent Event, MessageInfo Errors, DBObject OldObject)
		{
			base.OnConfigChange(Event, Errors, OldObject);
		}

		public override void OnValidateConfig(MessageInfo Errors)
		{
			//ToDo Check topics not empty

			//Errors.Add("A config error messsage.");
			base.OnValidateConfig(Errors);
		}
	}

	[Table("SparkplugB Time", "SparkplugB")]
	public class SparkplugBPointTm : TimePoint
	{
		// Derived configuration field
		// This flag is stored and saved in the base class
		// To configure it it needs to be exposed by an override
		[Label("In Service", 1, 1)]
		[ConfigField("In Service",
		"Controls whether the point is active.",
		1, 2, 0x0350501B)]
		public override bool InService
		{
			get
			{
				return base.InService;
			}
			set
			{
				base.InService = value;
			}
		}

		[Label("Device", 2, 1)]
		[ConfigField("Scanner", "Scanner", 2, 2, 0x0350532F)]
		public new Reference<SparkplugBND> ScannerId
		{
			get
			{
				return new Reference<SparkplugBND>(base.ScannerId);
			}
			set
			{
				base.ScannerId = new Reference<Scanner>(value);
			}
		}

		[Label("Sparkplug Name", 2, 3)]
		[ConfigField("SparkplugName", "Sparkplug point name.", 2, 4, OPCProperty.Base + 74, Length = 40, AlwaysOverride = true)]
		public string SparkplugName;

		// Common per type
		[Label("Address Alias", 3, 1)]
		[ConfigField("Address",
					 "The numeric address alias of the point",
					 3, 2, OPCProperty.Base + 6)]
		public int Address;

		// Read-only data field containing a type Name
		[Label("Sparkplug Datatype", 3, 3)]
		[ConfigField("SPtype", "Point Type Number", 3, 4, OPCProperty.Base + 72, DefaultOverride = true)]
		public int SPtype;

		// Group of configuration fields
		// Normally an attribute on the first config field of the group
		[Label("Historic Data Filter", 4, 1, Width = 2, Height = 4)]
		// Historic filter is indexed property
		// Expose each item as a single tick box
		[Label("Significant Change", 5, 1)]
		[ConfigField("Report Historic Filter",
					"Controls whether Significant Value Changes are logged historically",
					5, 2, 0x3506913)]
		public bool HistoricFilterValue
		{
			get
			{
				return base.get_HistoricFilter(PointSourceEntry.Reason.ValueChange);
			}
			set
			{
				base.set_HistoricFilter(PointSourceEntry.Reason.ValueChange, value);
			}
		}
		[Label("State Change", 6, 1)]
		[ConfigField("Report Historic Filter",
					"Controls whether State Changes are logged historically",
					6, 2, 0x3506914)]
		public bool HistoricFilterState
		{
			get
			{
				return base.get_HistoricFilter(PointSourceEntry.Reason.StateChange);
			}
			set
			{
				base.set_HistoricFilter(PointSourceEntry.Reason.StateChange, value);
			}
		}
		[Label("Report", 7, 1)]
		[ConfigField("Report Historic Filter",
				"Controls whether Timed Report values are logged historically",
				7, 2, 0x3506915)]
		public bool HistoricFilterReport
		{
			get
			{
				return base.get_HistoricFilter(PointSourceEntry.Reason.Report);
			}
			set
			{
				base.set_HistoricFilter(PointSourceEntry.Reason.Report, value);
			}
		}
		[Label("End of Period Report", 8, 1)]
		[ConfigField("EOP Historic Filter",
				"Controls whether End of Period values are logged historically",
				8, 2, 0x3506916)]
		public bool HistoricFilterEOP
		{
			get
			{
				return base.get_HistoricFilter(PointSourceEntry.Reason.EndofPeriod);
			}
			set
			{
				base.set_HistoricFilter(PointSourceEntry.Reason.EndofPeriod, value);
			}
		}

		public override void OnConfigChange(ConfigEvent Event, MessageInfo Errors, DBObject OldObject)
		{
			base.OnConfigChange(Event, Errors, OldObject);
		}

		public override void OnValidateConfig(MessageInfo Errors)
		{
			//ToDo Check topics not empty

			//Errors.Add("A config error messsage.");
			base.OnValidateConfig(Errors);
		}
	}

	[Table("SparkplugB Counter", "SparkplugB")]
	public class SparkplugBPointCi : CounterPoint
	{
		// Derived configuration field
		// This flag is stored and saved in the base class
		// To configure it it needs to be exposed by an override
		[Label("In Service", 1, 1)]
		[ConfigField("In Service",
		"Controls whether the point is active.",
		1, 2, 0x0350501B)]
		public override bool InService
		{
			get
			{
				return base.InService;
			}
			set
			{
				base.InService = value;
			}
		}

		[Label("Device", 2, 1)]
		[ConfigField("Scanner", "Scanner", 2, 2, 0x0350532F)]
		public new Reference<SparkplugBND> ScannerId
		{
			get
			{
				return new Reference<SparkplugBND>(base.ScannerId);
			}
			set
			{
				base.ScannerId = new Reference<Scanner>(value);
			}
		}

		[Label("Sparkplug Name", 2, 3)]
		[ConfigField("SparkplugName", "Sparkplug point name.", 2, 4, OPCProperty.Base + 74, Length = 40, AlwaysOverride = true)]
		public string SparkplugName;

		// Common per type
		[Label("Address Alias", 3, 1)]
		[ConfigField("AddressAlias",
					 "The numeric address alias of the point",
					 3, 2, OPCProperty.Base + 6)]
		public int Address;

		// Read-only data field containing a type Name
		[Label("Sparkplug Datatype", 3, 3)]
		[ConfigField("SPtype", "Point Type Number", 3, 4, OPCProperty.Base + 72, DefaultOverride = true)]
		public int SPtype;

		// Group of configuration fields
		// Normally an attribute on the first config field of the group
		[Label("Historic Data Filter", 4, 1, Width = 2, Height = 4)]
		// Historic filter is indexed property
		// Expose each item as a single tick box
		[Label("Significant Change", 5, 1)]
		[ConfigField("Report Historic Filter",
					"Controls whether Significant Value Changes are logged historically",
					5, 2, 0x3506913)]
		public bool HistoricFilterValue
		{
			get
			{
				return base.get_HistoricFilter(PointSourceEntry.Reason.ValueChange);
			}
			set
			{
				base.set_HistoricFilter(PointSourceEntry.Reason.ValueChange, value);
			}
		}
		[Label("State Change", 6, 1)]
		[ConfigField("Report Historic Filter",
					"Controls whether State Changes are logged historically",
					6, 2, 0x3506914)]
		public bool HistoricFilterState
		{
			get
			{
				return base.get_HistoricFilter(PointSourceEntry.Reason.StateChange);
			}
			set
			{
				base.set_HistoricFilter(PointSourceEntry.Reason.StateChange, value);
			}
		}
		[Label("Report", 7, 1)]
		[ConfigField("Report Historic Filter",
				"Controls whether Timed Report values are logged historically",
				7, 2, 0x3506915)]
		public bool HistoricFilterReport
		{
			get
			{
				return base.get_HistoricFilter(PointSourceEntry.Reason.Report);
			}
			set
			{
				base.set_HistoricFilter(PointSourceEntry.Reason.Report, value);
			}
		}
		[Label("End of Period Report", 8, 1)]
		[ConfigField("EOP Historic Filter",
				"Controls whether End of Period values are logged historically",
				8, 2, 0x3506916)]
		public bool HistoricFilterEOP
		{
			get
			{
				return base.get_HistoricFilter(PointSourceEntry.Reason.EndofPeriod);
			}
			set
			{
				base.set_HistoricFilter(PointSourceEntry.Reason.EndofPeriod, value);
			}
		}

		public override void OnConfigChange(ConfigEvent Event, MessageInfo Errors, DBObject OldObject)
		{
			base.OnConfigChange(Event, Errors, OldObject);
		}

		public override void OnValidateConfig(MessageInfo Errors)
		{
			//ToDo Check topics not empty

			//Errors.Add("A config error messsage.");
			base.OnValidateConfig(Errors);
		}
	}
}
